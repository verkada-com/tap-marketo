import csv
import json
from pydoc import cli
import pendulum
import tempfile

import singer
from singer import metadata
from singer import bookmarks
from singer import utils
from tap_marketo.client import ExportFailed, ApiQuotaExceeded

# We can request up to 30 days worth of activities per export.
MAX_EXPORT_DAYS = 30

BASE_ACTIVITY_FIELDS = [
    "marketoGUID",
    "leadId",
    "activityDate",
    "activityTypeId",
]

ACTIVITY_FIELDS = BASE_ACTIVITY_FIELDS + [
    "primaryAttributeValue",
    "primaryAttributeValueId",
    "attributes",
]

def determine_replication_key(tap_stream_id):
    if tap_stream_id.startswith("activities_"):
        return 'activityDate'
    elif tap_stream_id == 'activity_types':
        return None
    elif tap_stream_id == 'leads':
        return 'updatedAt'
    elif tap_stream_id == 'lists':
        return 'updatedAt'
    elif tap_stream_id == 'campaigns':
        return 'updatedAt'
    elif tap_stream_id == 'programs':
        return 'updatedAt'
    else:
        return None


NO_ASSET_MSG = "No assets found for the given search criteria."
NO_CORONA_WARNING = (
    "Your account does not have Corona support enabled. Without Corona, each sync of "
    "the Leads table requires a full export which can lead to lower data freshness. "
    "Please contact Marketo to request Corona support be added to your account."
)
ITER_CHUNK_SIZE = 512

ATTRIBUTION_WINDOW_DAYS = 1


def format_value(value, schema):
    if not isinstance(schema["type"], list):
        field_type = [schema["type"]]
    else:
        field_type = schema["type"]

    if value in [None, "", 'null']:
        return None
    elif schema.get("format") == "date-time":
        return pendulum.parse(value).isoformat()
    elif "integer" in field_type:
        if isinstance(value, int):
            return value

        # Custom Marketo percent type fields can have decimals, so we drop them
        decimal_index = value.find('.')
        if decimal_index > 0:
            singer.log_warning("Dropping decimal from integer type. Original Value: %s", value)
            value = value[:decimal_index]
        return int(value)
    elif "string" in field_type:
        return str(value)
    elif "number" in field_type:
        return float(value)
    elif "boolean" in field_type:
        if isinstance(value, bool):
            return value
        return value.lower() == "true"

    return value


def format_values(stream, row):
    rtn = {}

    available_fields = []
    for entry in stream['metadata']:
        if len(entry['breadcrumb']) > 0 and (entry['metadata'].get('selected') or entry['metadata'].get('inclusion') == 'automatic'):
            available_fields.append(entry['breadcrumb'][-1])

    for field, schema in stream["schema"]["properties"].items():
        if field in available_fields:
            rtn[field] = format_value(row.get(field), schema)
    return rtn


def update_state_with_export_info(state, stream, bookmark=None, export_id=None, export_end=None):
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "export_id", export_id)
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "export_end", export_end)
    if bookmark:
        state = bookmarks.write_bookmark(state, stream["tap_stream_id"], determine_replication_key(stream['tap_stream_id']), bookmark)

    singer.write_state(state)
    return state


def get_export_end(export_start, end_days=MAX_EXPORT_DAYS):
    export_end = export_start.add(days=end_days)
    if export_end >= pendulum.utcnow():
        export_end = pendulum.utcnow()

    return export_end.replace(microsecond=0)


def wait_for_export(client, state, stream, export_id):
    stream_type = "activities" if stream["tap_stream_id"] != "leads" else "leads"
    try:
        client.wait_for_export(stream_type, export_id)
    except ExportFailed:
        state = update_state_with_export_info(state, stream)
        raise

    return state

MEGABYTE_IN_BYTES = 1024 * 1024
CHUNK_SIZE_MB = 10
CHUNK_SIZE_BYTES = MEGABYTE_IN_BYTES * CHUNK_SIZE_MB

# This function has an issue with UTF-8 data most likely caused by decode_unicode=True
# See https://github.com/singer-io/tap-marketo/pull/51/files
def stream_rows(client, stream_type, export_id):
    with tempfile.NamedTemporaryFile(mode="w+", encoding="utf8") as csv_file:
        singer.log_info("Download starting.")
        resp = client.stream_export(stream_type, export_id)
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE_BYTES, decode_unicode=True):
            if chunk:
                # Replace CR
                chunk = chunk.replace('\r', '')
                csv_file.write(chunk)

        singer.log_info("Download completed. Begin streaming rows.")
        csv_file.seek(0)

        reader = csv.reader((line.replace('\0', '') for line in csv_file), delimiter=',', quotechar='"')
        headers = next(reader)
        for line in reader:
            yield dict(zip(headers, line))


def get_or_create_export_for_leads(client, state, stream, export_start, config):
    export_id = bookmarks.get_bookmark(state, "leads", "export_id")
    # check if export is still valid
    if export_id is not None and not client.export_available("leads", export_id):
        singer.log_info("Export %s no longer available.", export_id)
        export_id = None

    if export_id is None:
        # Corona mode is required to query by "updatedAt", otherwise a full
        # sync is required using "createdAt".
        query_field = "updatedAt" if client.use_corona else "createdAt"
        max_export_days = int(config.get('max_export_days',
                                         MAX_EXPORT_DAYS))
        export_end = get_export_end(export_start,
                                    end_days=max_export_days)
        query = {query_field: {"startAt": export_start.isoformat(),
                               "endAt": export_end.isoformat()}}

        # Create the new export and store the id and end date in state.
        # Does not start the export (must POST to the "enqueue" endpoint).
        fields = []
        for entry in stream['metadata']:
            if len(entry['breadcrumb']) > 0 and (entry['metadata'].get('selected') or entry['metadata'].get('inclusion') == 'automatic'):
                fields.append(entry['breadcrumb'][-1])

        export_id = client.create_export("leads", fields, query)
        state = update_state_with_export_info(
            state, stream, export_id=export_id, export_end=export_end.isoformat())
    else:
        export_end = pendulum.parse(bookmarks.get_bookmark(state, "leads", "export_end"))

    return export_id, export_end


def get_or_create_export_for_activities(client, state, stream, export_start, config):
    export_id = bookmarks.get_bookmark(state, stream["tap_stream_id"], "export_id")
    if export_id is not None and not client.export_available("activities", export_id):
        singer.log_info("Export %s no longer available.", export_id)
        export_id = None

    if export_id is None:
        # The activity id is in the top-most breadcrumb of the metatdata
        # Activity ids correspond to activity type id in Marketo.
        # We need the activity type id to build the query.
        activity_metadata = metadata.to_map(stream["metadata"])
        activity_type_id = metadata.get(activity_metadata, (), 'marketo.activity-id')

        # Activities must be queried by `createdAt` even though
        # that is not a real field. `createdAt` proxies `activityDate`.
        # The activity type id must also be included in the query. The
        # largest date range that can be used for activities is 30 days.
        max_export_days = int(config.get('max_export_days',
                                         MAX_EXPORT_DAYS))
        export_end = get_export_end(export_start,
                                    end_days=max_export_days)
        query = {"createdAt": {"startAt": export_start.isoformat(),
                               "endAt": export_end.isoformat()},
                 "activityTypeIds": [activity_type_id]}

        # Create the new export and store the id and end date in state.
        # Does not start the export (must POST to the "enqueue" endpoint).
        try:
            export_id = client.create_export("activities", ACTIVITY_FIELDS, query)
        except ApiQuotaExceeded as e:
            # The main reason we wrap the ApiQuotaExceeded exception in a
            # new one is to be able to tell the customer what their
            # configured max_export_days is.
            raise ApiQuotaExceeded(
                ("You may wish to consider changing the "
                 "`max_export_days` config value to a lower number if "
                 "you're unable to sync a single {} day window within "
                 "your current API quota.").format(
                     max_export_days)) from e
        state = update_state_with_export_info(
            state, stream, export_id=export_id, export_end=export_end.isoformat())
    else:
        export_end = pendulum.parse(bookmarks.get_bookmark(state, stream["tap_stream_id"], "export_end"))

    return export_id, export_end


def flatten_activity(row, stream):
    # Start with the base fields
    rtn = {field: row[field] for field in BASE_ACTIVITY_FIELDS}

    # Add the primary attribute name
    # This name is the human readable name/description of the
    # pimaryAttribute
    mdata = metadata.to_map(stream['metadata'])
    pan_field = metadata.get(mdata, (), 'marketo.primary-attribute-name')
    if pan_field:
        rtn['primary_attribute_name'] = pan_field
        rtn['primary_attribute_value'] = row['primaryAttributeValue']
        rtn['primary_attribute_value_id'] = row['primaryAttributeValueId']

    # Now flatten the attrs json to it's selected columns
    #TODO : figure out how to get attributes...
    # if "attributes" in row:
    #     attrs = json.loads(row["attributes"])
    #     for key, value in attrs.items():
    #         key = key.lower().replace(" ", "_")
    #         rtn[key] = value

    return rtn


# def sync_leads(client, state, stream, config):
#     # http://developers.marketo.com/rest-api/bulk-extract/bulk-lead-extract/
#     replication_key = determine_replication_key(stream["tap_stream_id"])

#     singer.write_schema("leads", stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
#     initial_bookmark = pendulum.parse(bookmarks.get_bookmark(state, "leads", replication_key))
#     export_start = pendulum.parse(bookmarks.get_bookmark(state, "leads", replication_key))
#     if client.use_corona:
#         export_start = export_start.subtract(days=ATTRIBUTION_WINDOW_DAYS)

#     job_started = pendulum.utcnow()
#     record_count = 0
#     max_bookmark = initial_bookmark
#     while export_start < job_started:
#         export_id, export_end = get_or_create_export_for_leads(client, state, stream, export_start, config)
#         state = wait_for_export(client, state, stream, export_id)
#         for row in stream_rows(client, "leads", export_id):
#             time_extracted = utils.now()

#             record = format_values(stream, row)
#             record_bookmark = pendulum.parse(record[replication_key])

#             if client.use_corona:
#                 max_bookmark = export_end

#                 singer.write_record("leads", record, time_extracted=time_extracted)
#                 record_count += 1
#             elif record_bookmark >= initial_bookmark:
#                 max_bookmark = max(max_bookmark, record_bookmark)

#                 singer.write_record("leads", record, time_extracted=time_extracted)
#                 record_count += 1

#         # Now that one of the exports is finished, update the bookmark
#         state = update_state_with_export_info(state, stream, bookmark=max_bookmark.isoformat())
#         export_start = export_end

#     return state, record_count


def sync_activities(client, state, stream, config):
    # http://developers.marketo.com/rest-api/bulk-extract/bulk-activity-extract/
    replication_key = determine_replication_key(stream['tap_stream_id'])
    singer.write_schema(stream["tap_stream_id"], stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    export_start = pendulum.parse(bookmarks.get_bookmark(state, stream["tap_stream_id"], replication_key))
    job_started = pendulum.utcnow()
    record_count = 0
    while export_start < job_started:
        export_id, export_end = get_or_create_export_for_activities(client, state, stream, export_start, config)
        state = wait_for_export(client, state, stream, export_id)
        for row in stream_rows(client, "activities", export_id):
            time_extracted = utils.now()

            row = flatten_activity(row, stream)
            record = format_values(stream, row)

            singer.write_record(stream["tap_stream_id"], record, time_extracted=time_extracted)
            record_count += 1

        state = update_state_with_export_info(state, stream, bookmark=export_start.isoformat())
        export_start = export_end

    return state, record_count


def sync_programs(client, state, stream):
    # http://developers.marketo.com/rest-api/assets/programs/#by_date_range
    #
    # Programs are queryable via their updatedAt time but require and
    # end date as well. As there is no max time range for the query,
    # query from the bookmark value until current.
    #
    # The Programs endpoint uses offsets with a return limit of 200
    # per page. If requesting past the final program, an error message
    # is returned to indicate that the endpoint has been fully synced.
    replication_key = determine_replication_key(stream['tap_stream_id'])

    singer.write_schema("programs", stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    start_date = bookmarks.get_bookmark(state, "programs", replication_key)
    end_date = pendulum.utcnow().isoformat()
    params = {
        "maxReturn": 200,
        "offset": 0,
        "earliestUpdatedAt": start_date,
        "latestUpdatedAt": end_date,
    }
    endpoint = "rest/asset/v1/programs.json"

    record_count = 0
    while True:
        data = client.request("GET", endpoint, endpoint_name="programs", params=params)

        # If the no asset message is in the warnings, we have exhausted
        # the search results and can end the sync.
        if "warnings" in data and NO_ASSET_MSG in data["warnings"]:
            break

        time_extracted = utils.now()

        # Each row just needs the values formatted. If the record is
        # newer than the original start date, stream the record.
        for row in data["result"]:
            record = format_values(stream, row)
            if record[replication_key] >= start_date:
                record_count += 1

                singer.write_record("programs", record, time_extracted=time_extracted)

        # Increment the offset by the return limit for the next query.
        params["offset"] += params["maxReturn"]

    # Now that we've finished every page we can update the bookmark to
    # the end of the query.
    state = bookmarks.write_bookmark(state, "programs", replication_key, end_date)
    singer.write_state(state)
    return state, record_count


def sync_leads_paginated(client, state, stream):
    # http://developers.marketo.com/documentation/rest/get-multiple-leads-by-list-id/
    #
    # This is a supplementary sync for Lead. We're Marketo Static List 13702 to give us newly updated leads
    # Leads are paginated with a max return of 300 items per page 
    replication_key = determine_replication_key(stream['tap_stream_id'])
    fields = ",".join(list(stream["schema"]["properties"].keys()))
    singer.log_info(f"Lead fields is :{fields}")

    test = """company,site,billingStreet,billingCity,billingState,billingCountry,billingPostalCode,website,mainPhone,annualRevenue,numberOfEmployees,industry,sicCode,mktoCompanyNotes,sfdcAccountId,externalCompanyId,id,mktoName,personType,mktoIsPartner,isLead,mktoIsCustomer,isAnonymous,salutation,firstName,middleName,lastName,email,phone,mobilePhone,fax,title,contactCompany,dateOfBirth,address,city,state,country,postalCode,originalSourceType,originalSourceInfo,registrationSourceType,registrationSourceInfo,originalSearchEngine,originalSearchPhrase,originalReferrer,emailInvalid,emailInvalidCause,unsubscribed,unsubscribedReason,doNotCall,mktoDoNotCallCause,doNotCallReason,marketingSuspended,marketingSuspendedCause,blackListed,blackListedCause,mktoPersonNotes,sfdcType,sfdcContactId,anonymousIP,inferredCompany,inferredCountry,inferredCity,inferredStateRegion,inferredPostalCode,inferredMetropolitanArea,inferredPhoneAreaCode,emailSuspended,emailSuspendedCause,emailSuspendedAt,department,sfdcId,createdAt,updatedAt,cookies,externalSalesPersonId,leadPerson,leadRole,leadSource,leadStatus,leadScore,urgency,priority,relativeScore,relativeUrgency,rating,sfdcLeadId,sfdcLeadOwnerId,personPrimaryLeadInterest,leadPartitionId,leadRevenueCycleModelId,leadRevenueStageId,gender,facebookDisplayName,twitterDisplayName,linkedInDisplayName,facebookProfileURL,twitterProfileURL,linkedInProfileURL,facebookPhotoURL,twitterPhotoURL,linkedInPhotoURL,facebookReach,twitterReach,linkedInReach,facebookReferredVisits,twitterReferredVisits,linkedInReferredVisits,totalReferredVisits,facebookReferredEnrollments,twitterReferredEnrollments,linkedInReferredEnrollments,totalReferredEnrollments,lastReferredVisit,lastReferredEnrollment,syndicationId,facebookId,twitterId,linkedInId,acquisitionProgramId,mktoAcquisitionDate,MiddleName_lead,Suffix,Latitude,Longitude,GeocodeAccuracy,Address_lead,PhotoUrl,LastViewedDate,LastReferencedDate,Jigsaw,JigsawContactId_lead,EmailBouncedReason,EmailBouncedDate,Lead_Source_Detail__c,Flow_Open_MQL__c,Flow_MQL_Contacted__c,Flow_Contacted_SQL__c,Flow_Date_Unqualified__c,Count_as_MQL__c,Count_as_Contacted__c,Count_as_SQL__c,Count_as_Nurtured__c,Lead_Sub_Status__c,No_of_Employees_range__c,Source_List_Name__c,Email_Address_Status__c,Email_Address_Last_Verified__c,LinkedIn_Profile__c,LinkedIn_Profile_Location__c,Phone_Type__c,Exclude_from_Lead_Flow_Rules__c,Count_as_Unqualified__c,BillingLatitude,BillingLongitude,BillingGeocodeAccuracy,BillingAddress,ShippingLatitude,ShippingLongitude,ShippingGeocodeAccuracy,ShippingAddress,PhotoUrl_account,LastViewedDate_account,LastReferencedDate_account,Jigsaw_account,JigsawCompanyId_account,AccountSource,SicDesc,No_of_Employees_range__c_account,MailingLatitude,MailingLongitude,MailingGeocodeAccuracy,MailingAddress,IsEmailBounced,Company_Size_range__c,Company_Size_range__c_account,Trial_Form_Number_of_Cameras__c,Partners_Years_in_Business__c,Partners_Office_Locations__c,Partners_Annual_Revenue__c,Partners_Top_5__c,Partners_Agree_to_Terms__c,rkpi2__DeletedFromRainKing__c,rkpi2__rkCompanyId__c,rkpi2__rkContactId__c,rkpi2__rk_DefaultVisibility__c,rkpi2__rk_RetrievalFlag__c,rkpi2__DeletedFromRainKing__c_account,rkpi2__rkCompanyId__c_account,rkpi2__rk_DefaultVisibility__c_account,rkpi2__rk_RetrievalFlag__c_account,Original_Conversion_Channel__c,RecordTypeId,UTM_Campaign__c,UTM_Source__c,UTM_Medium__c,UTM_gclid__c,Contact_Verkada_Reason__c,Contact_Verkada_Comments__c,personTimeZone,bVaccount,bVaddress,bVdisposable,bVdomain,bVduration,bVerror,bVerrorcode,bVroleaddress,bVstatus,zoomJoinUrl,webinarTitle,Trial_Form_No_of_Locations__c,Command_User_Login__c,Command_Temp_Password__c,Form_Yes_to_Free_Camera__c,Total_Company_Locations__c,Total_Company_Locations__c_account,Free_Camera_Approved__c,Record_Type__c,Free_Camera_Plugged_In__c,HQ_State__c,Total_Bookings_Marketing__c,Total_Bookings_Partners__c,Total_Bookings_Outbound_Sales__c,Shipping_Address_Confirmed__c,HQ_State__c_account,Customer_ID__c,Form_Yes_to_PoE_Injector__c,Form_PoE_Injector__c,Lead_Counter__c,Round_Robin_Sales_Number__c,DSCORGPKG__Company_HQ_Address__c,DSCORGPKG__Company_HQ_City__c,DSCORGPKG__Company_HQ_Country_Code__c,DSCORGPKG__Company_HQ_Country_Full_Name__c,DSCORGPKG__Company_HQ_Postal_Code__c,DSCORGPKG__Company_HQ_State_Full_Name__c,DSCORGPKG__Company_HQ_State__c,DSCORGPKG__Company_Phone__c,DSCORGPKG__Conflict__c,DSCORGPKG__DO_3yr_Employees_Growth__c,DSCORGPKG__DO_3yr_Sales_Growth__c,DSCORGPKG__DeletedFromDiscoverOrg__c,DSCORGPKG__DiscoverOrg_Company_ID__c,DSCORGPKG__DiscoverOrg_Created_On__c,DSCORGPKG__DiscoverOrg_First_Update__c,DSCORGPKG__DiscoverOrg_FullCountryName__c,DSCORGPKG__DiscoverOrg_ID__c,DSCORGPKG__DiscoverOrg_LastUpdate__c,DSCORGPKG__DiscoverOrg_State_Full_Name__c,DSCORGPKG__DiscoverOrg_Technologies__c,DSCORGPKG__Email_Invalid__c,DSCORGPKG__Exclude_Update__c,DSCORGPKG__External_DiscoverOrg_Id__c,DSCORGPKG__Fiscal_Year_End__c,DSCORGPKG__Fortune_Rank__c,DSCORGPKG__ITOrgChart__c,DSCORGPKG__IT_Budget__c,DSCORGPKG__IT_Employees__c,DSCORGPKG__Job_Function__c,DSCORGPKG__LinkedIn_URL__c,DSCORGPKG__Locked_By_User__c,DSCORGPKG__Management_Level__c,DSCORGPKG__MiddleName__c,DSCORGPKG__NAICS_Codes__c,DSCORGPKG__NickName__c,DSCORGPKG__Other_Phone__c,DSCORGPKG__Ownership__c,DSCORGPKG__ReportsTo__c,DSCORGPKG__SIC_Codes__c,DSCORGPKG__Twitter_URL__c,DSCORGPKG__department__c
    ,DSCORGPKG__Conflict__c_account,DSCORGPKG__DO_3yr_Employees_Growth__c_account,DSCORGPKG__DO_3yr_Sales_Growth__c_account,DSCORGPKG__DeletedFromDiscoverOrg__c_account,DSCORGPKG__DiscoverOrg_Created_On__c_account,DSCORGPKG__DiscoverOrg_First_Update__c_account,DSCORGPKG__DiscoverOrg_FullCountryName__c_account,DSCORGPKG__DiscoverOrg_ID__c_account,DSCORGPKG__DiscoverOrg_LastUpdate__c_account,DSCORGPKG__DiscoverOrg_State_Full_Name__c_account,DSCORGPKG__DiscoverOrg_Technologies__c_account,DSCORGPKG__Exclude_Update__c_account,DSCORGPKG__External_DiscoverOrg_Id__c_account,DSCORGPKG__Fiscal_Year_End__c_account,DSCORGPKG__Fortune_Rank__c_account,DSCORGPKG__ITOrgChart__c_account,DSCORGPKG__IT_Budget__c_account,DSCORGPKG__IT_Employees__c_account,DSCORGPKG__Lead_Source__c,DSCORGPKG__Locked_By_User__c_account,DSCORGPKG__NAICS_Codes__c_account,DSCORGPKG__Ownership__c_account,DSCORGPKG__SIC_Codes__c_account,DSCORGPKG__Exclude_Update__c_contact,DSCORGPKG__REMOVELinkedinURL__c,DSCORGPKG__title_Custom__c,Has_Opportunity__c,Form_D30s__c,Form_D50s__c,Form_License_Term__c,tShirtSize,Cadence_Stage__c,Partner_Status__c,Partners_Agree_to_Terms__c_account,Partners_Annual_Revenue__c_account,Partners_Office_Locations__c_account,Partners_Top_5_Vendors__c,Partners_Years_in_Business__c_account,Partner_Services_Offered__c,Form_Ref_Code__c,Partner_Agreement_Signed_Date__c,Account_Lead_Source__c,Lifetime_Account_Bookings__c,Lead_Source_Text__c,Account_Source_Text__c,Campaign_Lead_Source__c,Partners_Deal_Reg_Partner__c,Partners_Deal_Reg_Rep__c,UTM_Content__c,UTM_Adset__c,Lead_Source_Origin__c,Lead_Source_Last_Touch__c,promoShippingTracking,Funnel_Type__c,Client_Status__c,Behavioral_Score__c,Demographic_Score__c,Client_Score__c,MQL_Date__c,Audience_Segment__c,MEL_Date__c,Partner_Application_Form_Terms__c,Partner_Application_Status__c,Referring_Sales_Rep__c,MQL_Reason__c,Referred_Project_Details__c,Referred_No_Cameras__c,Referred_No_Locations__c,GCLID__c,Entrance_URL__c,Conversion_URL__c,UTM_Term__c,Most_Recent_Campaign__c,triggerRouting,Partners_Does_Installs__c,Partners_Does_Installs__c_account,Notes__c,Trigger_Sequence__c,HQ_Postal_Code__c,Last_Opportunity_Close__c,interestingMomentsHistory,webPageHistory,MQL_Review__c,FT_Lead_Source__c,FT_Lead_Source__c_contact,MK_Customer_Fit_Segment__c,MK_Customer_Fit_Score__c,MK_Customer_Fit__c,MK_Customer_Fit_Signals__c,mostRecentForm,Referrer_Name__c,Activities_Since_MQL__c,marketingCampaignStatus,mkto_si__Add_to_Marketo_Campaign__c,mkto_si__HideDate__c,mkto_si__MSIContactId__c,mkto_si__Mkto_Lead_Score__c,mkto_si__Sales_Insight__c,Partner_Shipping_Email_Notification__c,NAICS_Code__c,Sub_Industry__c,NAICS_Code__c_account,Sub_Industry__c_account,HQ_Postal_Code__c_account,Partner_Segment__c,Partner_Deal_Reg_Status__c,Holdover_Requested__c,Holdover_Status__c,Holdover_Expiration__c,Unqualified_Reason__c,Sequence_Active__c,mk_customer_fit_segment__c_contact,mk_customer_fit_score__c_contact,mk_customer_fit__c_contact,mk_customer_fit_signals__c_contact,Referrer_Phone__c,Call_Progress__c,cbit__ClearbitReady__c,cbit__Clearbit__c,cbit__CreatedByClearbit__c,cbit__Facebook__c,cbit__LinkedIn__c,cbit__Twitter__c,cbit__ClearbitDomain__c,cbit__ClearbitReady__c_account,cbit__Clearbit__c_account,cbit__CreatedByClearbit__c_account,cbit__ClearbitReady__c_contact,clearbitFormStatus,rcsfl__SMS_Number__c,rcsfl__SendSMS__c,brightTALKUserID,brightTALKActivtyType,brightTALKWebcastClientBookingReference,brightTALKWebcastName,Secondary_Email__c,Lead__c,First_Website_Referrer_FWR__c,Lead_Notes__c,Contact_Notes__c,Was_Lead__c,First_Opportunity__c,Lead_Tracking_Stamp_Date__c,Lead_Creation_Mechanism_LCM1__c,First_Website_Referrer_FWR1__c,Lead_Tracking_Stamp_Date__c_account,neverBounceValidationResult,Email_Domain__c,fBMostFrustratingThing,fBCurrentVendor,fBTypeofSecurity,fBAreYouLookingtoUpgrade,LinkedIn_Industry__c,Total_EC_Bookings__c,Total_EC_MSRP__c,Average_Discount_EC_Only__c,Reseller_Partner__c,Raw_LeadGen_Form_Email__c,Partners_Agreed_to_Terms_Date__c,linkedInLeadGenWorkEmail,Outstanding_Trial_Orders__c,mostRecentFormURL,Marketing_Utility_One__c,CB_Employee_Count__c,MO_Employee_Count__c,Employee_Count__c,MO_Student_Count__c,NCES_Student_Count__c,Student_Count__c,marketoEmployeeCount,NCES_ID__c,Employee_Count__c_account,OS_Student_Count__c,linkedInLeadGenCompanySize,CB_Employee_Count__c_account,MO_Employee_Count__c_account,MO_Student_Count__c_account,NCES_Student_Count__c_account,OS_Employee_Count__c,OS_Student_Count__c_account,Name_of_Currently_Active_Sequence__c,Current_Sequence_Status__c,Current_Sequence_Step_Number__c,Current_Sequence_Step_Type__c,Current_Sequence_Task_Due_Date__c,Current_Sequence_User_SFDC_ID__c,Current_Sequence_Name__c,Number_of_Active_Sequences__c,Last_Email_Delivered__c,Name_of_Currently_Active_Sequence__c_contact,Current_Sequence_Name__c_contact,Student_Count__c_account,linkedInLeadGenPhone,Prevent_Marketo_Sync__c,SQL_Date__c,DO_Employee_Count__c,DO_Employee_Count__c_account,marketoStudentCount,Reseller_Credit_Limit__c,Marketing_Utility_Two__c,SDR_Owner__c,SQL_Owner__c,BANT_Budget__c,BANT_Authority__c,BANT_Needs__c,BANT_Timing__c,Proposed_Meeting_Date__c,currentVideoSecuritySolution,Has_Had_Trial__c,IsPartner,Trial_Count__c,Command_User__c,Partner_rebate_opt_in__c,Call_Progress_Last_Updated_Date__c,Unqualified_Date__c,Suspended_Date__c,lookingtoupgradesecuritynext12mo,LastTaskActivityDate__c,LastEventActivityDate__c,Last_Sales_Activity_Date__c,Last_Prospect_Reply_Date__c,LinkedIn_Company_Size_Range__c,mostFrustratingPartofCurrentSecurity,whichVendorCurrentlyUsing,wouldBeOpenToChatWithVerkadaAboutSecurity,areYouLookingToUpgradeSecurityNext12Months,whatIsTheMostFrustratingPartOfCurrentSecurity,fBWorkPhoneNumber,CB_NAICS__c,CB_NAICS__c_account,lIMostFrustratingPartOfCurrentSecuritySystem,lICurrentSecurityVendor,lILookingToUpgradeVideoSecurityNext12Months,Last_scrubbed_date__c,fBInterestedInFreeTrialCamera,MO_NAICS_Code__c,Verkada_Industry_ID__c,Last_Client_Status_Modification_Date__c,From_Prospect__c,Verkada_Industry__c,NAICS__c,CB_HQ_State__c,CB_HQ_Postal_Code__c,MO_HQ_State__c,MO_HQ_Postal_Code__c,Clean_HQ_State__c,Clean_HQ_Postal_Code__c,OS_HQ_State__c,OS_HQ_Postal_Code__c,MO_HQ_State__c_account,MO_HQ_Postal_Code__c_account,CB_HQ_State__c_account,CB_HQ_Postal_Code__c_account,DO_HQ_State__c,DO_HQ_Postal_Code__c,MO_NAICS_Code__c_account,OS_HQ_State__c_account,OS_HQ_Postal_Code__c_account,Data_Review_Flag__c,Data_Review_Comment__c,Data_Review_Outcome__c,Data_Review_Outcome_Detail__c,NAICS__c_account,Clean_NAICS__c,zisf__Person_Has_Moved__c,zisf__ZoomInfo_Complete_Status__c,zisf__ZoomInfo_Email__c,zisf__ZoomInfo_Industry__c,zisf__ZoomInfo_Last_Clean_Run__c,zisf__ZoomInfo_Phone__c,zisf__ZoomInfo_Trigger__c,zisf__Zoom_Clean_Status__c,zisf__zoom_id__c,zisf__zoom_lastupdated__c,zisf__Company_ID__c,zisf__ZoomInfo_Complete_Status__c_account,zisf__ZoomInfo_Industry__c_account,zisf__ZoomInfo_Last_Clean_Run__c_account,zisf__Zoom_Clean_Status__c_account,zisf__zoom_id__c_account,zisf__zoom_lastupdated__c_account,Verkada_Industry__c_account,Clean_NAICS__c_lead,Reseller_Credit_Limit_Notes__c,Reseller_Partner_Hold__c,Deal_Reg_ID__c,Deal_Reg_ID__c_account,Estimated_Close_Date__c,Estimated_Deal_Size__c,Lead_Bucket__c,Last_Sales_Activity_Before_30_Days_Null__c,workEmail,From_Zoominfo__c,Is_Reseller__c,Partner_Ref_ID__c,Last_scrubbed_date__c_account,DO_HQ_State__c_lead,DO_HQ_Postal_Code__c_lead,Clean_HQ_State__c_account,Clean_HQ_Postal__c,Data_Flag_Owner__c,Data_Flag_Date__c,Data_Resolution_Date__c,Lead_Age__c,Partner_Ref_ID__c_account,Referral_Links_Partner__c,Referral_Links_Partner_Rep__c,Nurture_Segmentation__c,Holdover_Context__c,Territory_Exception__c,ZI_HQ_Postal_Code__c,ZI_HQ_State__c,ZI_NAICS_Code__c,ZI_Employee_Count__c,ZoomInfo_Company_ID__c,ZoomInfo_Company_Name__c,ZoomInfo_Last_Verified_Date__c,ZI_HQ_State__c_account,ZI_HQ_Postal_Code__c_account,ZI_NAICS_Code__c_account,ZI_Employee_Count__c_account,ZoomInfo_Company_ID__c_account,Lead_Account__c,ZoomInfo_Company_Name__c_account,ZoomInfo_Last_Verified_Date__c_account,ZI_HQ_Country__c,ZI_Industry__c,Email_at_Last_Sales_Activity_Date__c,Clean_HQ_Postal_Code__c_account,Demo_Progress__c,Demo_Type__c,Demo_Progress_Last_Updated_Date__c,Channel_Account_Manager_CAM__c,Channel_Account_Manager_at_Converted__c,SQL_Review_Status__c,SQL_Review_Comment__c,RecordTypeId_account,Clean_HQ_Country__c,Manual_Owner__c,Sales_Team_Segment__c,Route_Key__c,Clean_HQ_Country__c_lead,Territory_Owner__c,Sales_Team_Segment__c_lead,Route_Key__c_lead,Account__c,Account_HQ_Country__c,Account_HQ_State__c,Account_HQ_Postal__c,Account_Employee_Count__c,Account_Student_Count__c,Account_Sales_Segment__c,Account_ZoomInfo_Company_ID__c,OS_HQ_Country__c,Trigger_Routing__c,MO_HQ_Country__c,EmailDomainProxy__c,Manual_Owner_1__c,Total_Cameras_Purchased__c,lINumberofCameras,DealRegInitialAlertSent__c,agreetoEthicalBestPractices,Number_of_Leads__c,Data_Review_Flag__c_account,Data_Flag_Owner__c_account,Data_Flag_Date__c_account,Data_Review_Outcome__c_account,Data_Review_Outcome_Detail__c_account,Data_Resolution_Date__c_account,Data_Review_Comment__c_account,freeTrialRequestedFromShippingConfirmForm,NCES_HQ_State__c,NCES_HQ_Postal_Code__c,NCES_HQ_Country__c,Territory_Owner_Name__c,DealRegIndustry__c,Tier__c,Data_Review_Outcome_Detail__c_contact,sendosoTouchID,desiredGift,Create_Account_from_Lead__c,ZI_Do_Not_Clean__c,ManualAccount__c,OS_HQ_Country__c_lead,Original_Company__c,Opportunities_at_Qualified__c,Is_Holdover_Eligible__c,removeFromNurture,Customer_Segment__c,Apex_Route_Key__c,Apex_Sales_Team_Segment__c,Apex_Employee_Count__c,Apex_Student_Count__c,Account_Name__c,Number_of_Opportunities__c,Trial_Credit_Card_Requirement__c,Override_Trial_CC_and_Submit_for_Review__c,Override_Trial_CC_Requirement_Reason__c,Trial_CC_Bypass_Reason_Other_Comment__c,introWebinarLandingPageTestVersion,Renewal_Date__c,Total_Licenses__c,Account_Record_Type__c,Current_System__c,clean_naics_vs_naics_check__c,naics_code_vs_zi_naics_check__c,mo_naics_vs_clean_naics_check__c,MDR_Touch__c,MDR_Notes__c,Primary_Trial_Contact__c,Account_Notes__c,Open_Opportunity_Amount__c,trialEmailToSend,Delay_Holdover_Expiration__c,Has_Open_Trial__c,Preferred_Partner__c,Deal_Reg_Rate__c,trialCommunicationTrialLink,trialCommunicationsTrialOrderNumber,trialCommunicationsShipDate,trialCommunicationsTotalAmount,Previous_Owner__c,Wrong_owner__c,Account_owner_name__c,of_Locations__c,Account_Owner__c,Last_Updated_by_ObjectUpdate__c,of_cameras__c,Current_ACU__c,of_ACUs__c,ACU_Expiration__c,of_readers__c,Current_VMS__c,Dummy_Picklist__c,Partner_Segment_Region__c,Partner_Region__c,First_Opp_Date_IC__c,MO_Partner_region__c,First_Revenue_Opportunity__c,First_Revenue_Date__c,Incoming_Owner__c,Incoming_Owner_ID__c,
    MDR_Experiment_1__c,MDR_Experiment_1__c_lead,In_Transition__c,Target_Prospect__c,Target_Account__c,Most_Recent_Form__c,Original_Form__c,number_of_Locations__c,webinarGiftTrackingNumber,webinarGiftTrackingURL,cameraRecomendation,organizationDescription,videoStoragePreference,cameraDeploymentLocation,imageQualityImportance,storageRetentionRequirements,importantVideoSecurityFunctionality,budgetDescription,cameraSystemUpgradeReason,Renewal_Eligible_Products__c,Find_on_LinkedIn__c,Account_Industry__c,DaScoopComposer__Email_2__c,DaScoopComposer__Groove_Convert__c,DaScoopComposer__Groove_Log_a_Call__c,DaScoopComposer__Groove_Notes__c,DaScoopComposer__Normalized_Mobile__c,DaScoopComposer__Normalized_Phone__c,DaScoopComposer__Account_Tags__c,DaScoopComposer__Alias_1__c,DaScoopComposer__Alias_2__c,DaScoopComposer__Churned_Customer__c,DaScoopComposer__Domain_1__c,DaScoopComposer__Domain_2__c,DaScoopComposer__Dont_Match_Leads_to_this_Account__c,DaScoopComposer__Engagement_Status__c,DaScoopComposer__Groove_Notes__c_account,DaScoopComposer__Hash_1__c,DaScoopComposer__Hash_2__c,DaScoopComposer__Hash_3__c,DaScoopComposer__Inferred_Status__c,DaScoopComposer__Black_List__c,DaScoopComposer__Email_Domain__c,DaScoopComposer__Groove_Create_Opportunity__c,Personal_Email__c,Mobile_Phone__c,camerarecimg,Request_Account_Level_Discount_Approval__c,Account_Level_Discount__c,Requested_Account_Level_Discount__c,Case_Safe_ID__c,Viewing_Kit_Use_Case__c,Approve_Viewing_Kit_Use_Case__c,Created_by_lead_convert__c,Is_new_business__c,eventbriteID,Preferred_Distributor__c,Distributor_Type__c,Preferred_Distributor_Type__c,wereyoureferredbyaVerkadaAuthorizedReseller,emailofreferrer,Application_For_Viewing_Kit__c,DOZISF__ZoomInfo_Company_ID__c,DOZISF__ZoomInfo_First_Updated__c,DOZISF__ZoomInfo_Id__c,DOZISF__ZoomInfo_Last_Updated__c,DOZISF__ZoomInfo_First_Updated__c_account,DOZISF__ZoomInfo_Id__c_account,DOZISF__ZoomInfo_Last_Updated__c_account,Account_Type__c,Clean_HQ_Region__c,NeverbounceResult__c,NeverbounceLastDate__c,WebbulaResult__c,WebbulaLastDate__c,Team__c,Hierarchy__c,Function__c,Active__c,MDR_Experiment_2__c,MDR_Experiment_2_v2__c,ignoreLeadScoring,Reseller_Partner_Rep__c,Portal_Report_Identifier__c,Event_ID__c,Event_Registration_Status__c,Event_Checkin_Status__c,Event_Ticket_Type__c,Event_Ticket_Price__c,Event_Promo_Code__c,Event_Registration_Date__c,Event_Payment_Status__c,Event_Payment_Date__c,tempUnsubscribe,tempUnsubscribeTimestamp,eventSessionCheckinHistory,Age_Days__c,MMERP__FirstDatetimeRecv__c,MMERP__FirstDatetimeSent__c,MMERP__FirstDatetime__c,MMERP__FirstDirection__c,MMERP__FirstFromRecv__c,MMERP__FirstFromSent__c,MMERP__FirstFrom__c,MMERP__FirstIdRecv__c,MMERP__FirstIdSent__c,MMERP__FirstId__c,MMERP__FirstSubjectRecv__c,MMERP__FirstSubjectSent__c,MMERP__FirstSubject__c,MMERP__LastDatetimeRecv__c,MMERP__LastDatetimeSent__c,MMERP__LastDatetime__c,MMERP__LastDirection__c,MMERP__LastFromRecv__c,MMERP__LastFromSent__c,MMERP__LastFrom__c,MMERP__LastIdRecv__c,MMERP__LastIdSent__c,MMERP__LastId__c,MMERP__LastSubjectRecv__c,MMERP__LastSubjectSent__c,MMERP__LastSubject__c,MMERP__ResponseGap__c,MMERP__TotalRecv__c,MMERP__TotalSent__c,MMERP__Total__c,MMERP__FirstDatetimeRecv__c_account,MMERP__FirstDatetimeSent__c_account,MMERP__FirstDatetime__c_account,MMERP__FirstDirection__c_account,MMERP__FirstFromRecv__c_account,MMERP__FirstFromSent__c_account,MMERP__FirstFrom__c_account,MMERP__FirstIdRecv__c_account,MMERP__FirstIdSent__c_account,MMERP__FirstId__c_account,MMERP__FirstSubjectRecv__c_account,MMERP__FirstSubjectSent__c_account,MMERP__FirstSubject__c_account,MMERP__LastDatetimeRecv__c_account,MMERP__LastDatetimeSent__c_account,MMERP__LastDatetime__c_account,MMERP__LastDirection__c_account,MMERP__LastFromRecv__c_account,MMERP__LastFromSent__c_account,MMERP__LastFrom__c_account,MMERP__LastIdRecv__c_account,MMERP__LastIdSent__c_account,MMERP__LastId__c_account,MMERP__LastSubjectRecv__c_account,MMERP__LastSubjectSent__c_account,MMERP__LastSubject__c_account,MMERP__ResponseGap__c_account,MMERP__TotalRecv__c_account,MMERP__TotalSent__c_account,MMERP__Total__c_account,technologyAdviceContent,technologyAdviceCampaign,Partner_Account__c,Within_Deal_Reg_Window__c,Most_Recent_Deal_Reg_Expiration_Date_rol__c,Most_Recent_Deal_Reg_Start_Date_Rollup__c,alertType,Solutions_Engineer_Picklist__c,SE_Contribution__c,SE_Comments__c,Most_Recent_Deal_Reg_Start_Date__c,Most_Recent_Deal_Reg_End_Date__c,Account_Hyperlink__c,Tier__c_lead,Route_Owner_Segment__c,Route_Owner_Division__c,License_Extension_Requested_Until_Date__c,IsUK__c,Renewal_Email_Send_Date__c,Renewal_Email_Exemption__c,Deal_Reg_User_Location__c,Deal_Reg_Delete__c,Merge_Key__c,Merge_Master__c,Account_Approval_Status__c,Has_Manual_Owner__c,Bad_Zoominfo_ID_Match_Discovered__c,mostRecentWebinar,Interested_In_Cameras__c,Interested_In_Access_Control__c,Product_Interest__c,Account_Partner_Status__c,Convert_to_Contact__c,ZI_Domain__c,matchableEmailDomain,to_be_deleted,First_Purchase_Amount_30d_Window__c,First_Purchase_Amount_Same_Fisc_Window__c,First_Purchase_Date__c,StateCode,CountryCode,BillingStateCode,BillingCountryCode,ShippingStateCode,ShippingCountryCode,MailingStateCode,MailingCountryCode,Marketo_State_Province__c,Marketo_Country__c,ID_Case_Safe__c,Marketo_Do_Not_Merge__c,Country_Formula_Delete_Me__c,State_Formula_TEMP_DELETE_ME__c,Account_Id_Formula_delete_me__c,country_formula_delete_me__c_account,state_formula_delete_me__c,Sales_Outreach_Only_Until__c,webinardate,webinartime,Email_Bounced__c,emailBounces,hardEmailBounces,softEmailBounces,No_Bulk_Email__c,No_Bulk_Email_Tracking__c,fbclid__c,fbc__c,fbp__c,Payment_Plan_Exception__c,SDR_Prospect_Action__c,Is_ENT_SLED__c,Is_Customer__c,companyNameforCopy,Manually_Verified_Source__c,region,Lead_Tracking_Stamp_Date_Override__c,Company_GUID__c,Contact_GUID__c,IG_HQ_Country__c,IG_HQ_Employee_Count__c,IG_HQ_Postal_Code__c,IG_HQ_State__c,IG_HQ_Student_Count__c,Company_GUID__c_account,IG_HQ_Country__c_account,IG_HQ_Employee_Count__c_account,IG_HQ_Postal_Code__c_account,IG_HQ_State__c_account,IG_HQ_Student_Count__c_account,IG_HQ_Employee_Count__c_contact,IG_HQ_Student_Count__c_contact,SDR_Target_Account__c,latest_email_to_verkada_date,sessionid,wantitem,Number_Of_Locations_On_Account__c,Has_Written_Off_Opportunities__c,Total_Amount_Due__c,zoomSecondaryJoinUrl,Staging_Customer_Segment__c,Staging_Route_Key__c,Staging_Sales_Team_Segment__c,Needs_No_Bulk_Sync__c,ZerobounceResult__c,Last_Purchase_Date__c,Certifications__c,ZBMXRecord__c,ZBSMTPProvider__c,Current_Incoming_Owner_Different__c,Current_Staging_Owner_Different__c,First_Opportunity_Amount__c,Current_Staging_Incoming_Owner_Different__c,Request_CORP_SLED_Holdover__c,Request_CORP_SLED_Holdover_Reason__c,SLED_CORP_Personal_Filter__c,Average_Discount_for_EC_Opportunities__c,Lifetime_Deal_Reg_Bookings__c,Lifetime_Reseller_Bookings__c,Partner_Approval_Date__c,Partner_Level__c,Partner_Sub_Segment__c,Products_Installed__c,Products_Sold__c,Total_End_Customer_Licenses_Sold__c,Total_Open_Reseller_Opportunity_Amount__c,First_Deal_Registration_Date__c,Charge_Partner_Tax__c,Contracts_Held_by_Partner__c,Does_this_partner_allow_SPIFFs__c,Margin_Expectation__c,Marketing_Team__c,Must_receive_customer_tax_exemption_form__c,Partner_Account_LCM__c,Partner_Contracts_Verkada_is_On__c,Partner_Target_Market_Segment__c,Partner_Yearly_Deal_Reg_Commitment__c,Resale_Tax_Certificate__c,Self_Reported_Employee_Count__c,Verkada_Contracts_Partner_is_On__c,of_Sales_Employees__c,Lifetime_NFR_Bookings__c,NFR_Renewal_Date__c,Total_NFR_Licenses__c,Received_Test_Bypass__c,Partner_Type__c,Point_of_Contact__c,signalRecordType,signalCampaign,webinarStatus,eventStatus,eventNotes,truncatedWebHistory,webinarAttendanceTime,Demo_Status__c,Do_Not_Enrich_With_ZI__c,Do_Not_Enrich_With_ZI__c_account,testXCM,testFWR,Do_Not_Merge__c,randomstring,glocktestid,Last_Verkada_Action_Time__c,First_Touch_Date__c,Number_of_Touches__c,Time_Since_Last_Touch__c,XCM__c,XFWR__c,XCM__c_account,XFWR__c_account,Government_Level__c,Bypass_Account_Domain_Matching__c,Bypass_Account_Domain_Matching__c_account,Trigger_Outreach_Sequence__c_contact,Mk_Industry__c,ZI_Imported_By__c,ZI_Imported_By__c_contact,ZI_Imported_By_Current_User__c,Is_Batch_Converted__c,ZI_Imported_By_Email__c_contact,ZI_Imported_By_Email__c,Data_Source__c_contact,Population__c,Government_agency__c,Government_Agency__c_account,Is_End_Customer_Contact__c,Is_Initial_End_Customer_Contact__c,Converted_Lead__c,Converted_Lead_Id__c,First_Opportunity_Name_Standard__c,First_Opportunity_Type__c,LCM_Unlock_Notes__c,Contact_Status__c,X2020_12_audit_FWR_recommendation__c,X2020_12_audit_LCM_recommendation__c,Is_Duplicate__c,First_XCM_Date__c,Mailing_Address__c,Contact_Record_Updater__c,reddit_cid__c,founderName,Federal_ID_Number__c,CA_Primary_Contact_First_Name__c,CA_Primary_Contact_Last_Name__c,CA_Primary_Contact_Title__c,Accounting_Manager_Email_Address__c,CA_Phone_Number__c,CA_Primary_Contact_Email__c,Current_Cash_Balance__c,D_B_DUNS_Number__c,Email_To_Send_Invoices__c,Incorporated_Date__c,Mk_Seniority__c,Mk_Title__c,Last_3_Years_revenue__c,Last_Year_Revenue__c,X1_Company_Name__c,X1_Contact_Email__c,X1_Contact_First_and_Last_Name__c,X1_Contact_Phone_Number__c,X2_Company_Name__c,X2_Contact_Email__c,X2_Contact_First_and_Last_Name__c,X2_Contact_Phone_Number__c,Preclude_from_Certain_Outreach_Sequences__c,Verkada_Certified_Engineer__c,VCE_Date__c,Manually_Merge__c,Channel_Program_Trained_By__c,X2021_CARES_Fund_Amount__c,X2021_CARES_Fund_Amount__c_contact,Question_10_Value__c,Question_10__c,Question_11_Value__c,Question_11__c,Question_12_Value__c,Question_12__c,Question_13_Value__c,Question_13__c,Question_14_Value__c,Question_14__c,Question_15_Value__c,Question_15__c,Question_16_Value__c,Question_16__c,Question_17_Value__c,Question_17__c,Question_18_Value__c,Question_18__c,Question_19_Value__c,Question_19__c,Question_20_Value__c,Question_20__c,Question_21_Value__c,Question_21__c,Question_22_Value__c,Question_22__c,Question_23_Value__c,Question_23__c,Question_24_Value__c,Question_24__c,Question_25_Value__c,Question_25__c,Question_26__c,Question_26_Value__c,Question_27_Value__c,Question_27__c,Question_28_Value__c,Question_28__c,Question_29_Value__c,Question_29__c,Question_30_Value__c,Question_30__c,Question_8_Value__c,Question_8__c,Question_9_Value__c,Question_9__c,companyNameAC,Purchasing_Method__c,acNumofTechnicalEmployees,acSubcontractor,acFamiliarityLevel,acHasSelfInstalled,acPlatformsInstalled,acHasSold,acPlatformsSold,Is_Duplicate__c_account,Duplicate_Notes__c,Purchased_Cameras__c,Purchased_Access_Control__c,Purchased_Sensors__c,Cameras_on_Opps__c,Sensors_on_Opps__c,Access_Control_On_Opps__c,Renewal_details__c,Mops_Tag_Field__c,Renewal_Within_90_days__c,Cleaned_Company_Name__c,Reported_Duplicate_Account__c,X3_9_21_Security_Escalation__c,X3_9_21_Security_Escalation_Notes__c,Mops_Tag_Field__c_contact,X3_9_21_Security_Hold_Date__c,X3_9_21_Security_Hold_Notes__c,X3_9_21_Security_Hold_In_Place__c,Security_Escalation_Status__c,Last_Signal_Time__c,ecids,AC_Company_Has_Installed__c,Access_Control_Install_Certification_Dat__c,AC_Company_Has_Sold__c,AC_Familiarity_Number__c,AC_Partner_Level__c,AC_Quiz_Failed__c,AC_Quiz_Score__c,Failed_AC_Exam__c,Has_Installed_AC__c,Has_Sold_AC__c,Number_Of_Deal_Registrations_Submitted__c,Created_Date_of_Portal_User__c,Delegate_User__c,Last_Login_of_Portal_User__c,Number_of_Portal_User_Logins__c,Portal_User__c,AC_Partner_Level__c_account,Previous_Owner_Division__c,Holdover_Rationale__c,Request_Holdover__c,Request_AC_Cert_Upgrade__c,webinarCampaign,Question_31__c,Question_32__c,Question_33__c,Question_34__c,Question_35__c,Question_31_Value__c,Question_32_Value__c,Question_33_Value__c,Question_34_Value__c,Question_35_Value__c,signalCampaignName,Outreach_Last_Contacted_Date__c,MDR_Stale_Lead_Owner__c,MDR_Stale_Lead_Followup_Date__c,ESSER_III_Grant__c,Marketing_Utility_Three__c,TowerDataResult__c,MailgunResult__c,EmailValidationResult__c,ZerobounceSubStatus__c,MailgunRisk__c,HQ_Longitude__c,HQ_Latitude__c,Prospecting_Owner__c,Prospecting_Status__c,Outreach_Last_Contacted_Day__c,User_Identifier_Formula__c,Has_Attended_Webinar__c,Has_Attended_Webinar__c_contact,Opt_In_for_SPIFFs__c,Opted_In_for_SPIFFs__c,Opted_in_For_SPIFFs_Until__c,Deal_Regs_Submitted__c,Contracts_Held_by_Partner__c_lead,Zoom_Info_Last_Refreshed__c,Account_Access_Control_Discount__c,Account_Camera_Discount__c,Account_License_Discount__c,Account_Accessory_Discount__c,Account_Sensor_Discount__c,Account_Software_Discount__c,of_Sales_Employees__c_lead,Clean_Company_Name__c,Clean_Company_Name_from_Account__c,First_Revenue_Opportunity_Owner__c,Is_Holdover_Extension__c,Requested_Extension_Until__c,EC_Account_Status__c,Reseller_Account_Status__c,MDR_Status_Change__c,No_AAE_Free_Trial_Outreach__c,Last_Successful_AAE_Trial_Outreach__c,Legal_Decision_Maker__c,Esser_I_Grant__c,FY21_Esser_Grant_Total__c,FY_2021_SAMHSA_COVID_19_Funded_Grants__c,FY_2021_State_and_Local_Coronavirus_Fisc__c,FY_2021_American_Rescue_Plan_Act_Funding__c,FY_2020_Health_Center_Coronavirus_Aid_R__c,FY_2020_Coronavirus_Supplemental_Funding__c,FY_2020_Expanding_Capacity_for_Coronavir__c,FY_2020_Health_Center_Program_Look_Alike__c,FY_2020_Healtcare_Grant_Total__c,Acknowledged_Terms__c,Acknowledged_of_Monitoring_Services_A__c,Account_Alarm_Discount__c,Contact_Role__c,Contact_Sentiment__c,Owner_Email__c,Last_Verified_Date_Country__c,Last_Verified_Date_Postal__c,Last_Verified_Date_Verkada_Industry__c,Last_Verified_Date_Population__c,Last_Verified_Date_Student_Count__c,Last_Verified_Date_Employee_Count__c,Contact_Owner_Email__c,Current_Staging_Incoming_Owner_Diffe_del__c,Account_Short_Notes__c,All_Related_Notes__c,All_Account_Notes__c,AccountHasFirstOpp__c,IG_NAICS_Code__c,All_Contact_Notes__c,Contact_Short_Notes__c,Routed_By_AAE__c,Days_From_Last_Accepted_Deal_Reg__c,Most_Recent_Accepted_Deal_Reg_Date__c,Lifetime_NFR_Bookings_Rollup__c,Docusign_Agree_to_Term__c,First_Trial_Opportunity_Owner__c,All_Closed_Lost_Reasons__c,All_Next_Steps__c,mdrSequenceID,subprime_account__c,Days_Since_Last_Signal__c,Sops_Tag__c,First_Revenue_Opportunity_Owner_New__c,First_Trial_Opportunity__c,FY_2021_CPD_Allocation__c,Region__c,Support_Region__c,Hold_Status__c,Bypass_LCM9_Personal_Domain_Logic__c,Number_of_VCE_Contacts__c,Last_Email_Received_Datetime__c,Last_Email_Sent_Datetime__c,ownerphonenumber,Account_Is_Partner__c,Call_List_Until__c,Add_To_Call_List__c,Security_Incident_Closed_Lost_Pipeline__c,Prospecting_Owner_Role__c,Net_New_Cutoff_Date__c,EC_Owner_Concern__c,First_Revenue_Opp_LCM__c,First_Revenue_Opportunity_LCM__c,Partner_Account_Segment__c,F__c,FY_2021_PSGP_Allocation__c,FY_2021_TSGP_Allocation__c,FY_2021_IBSGP_Allocation__c,FY_2021_Tribal_Homeland_Security_Grant__c,signalExecutionARN,Dynamic_Prospecting_Status__c,Merge_Retry_Count__c,Person_Id__c,X2020_EANS_Allocation__c,Camera_Donation_Use_Case__c,Approve_Camera_Donation_Use_Case__c,Net_New_Deal_Reg_Account__c,Contact_Next_Steps_Date__c,HEER_II_Funding__c,reachdeskCampaignUUID,Alarm_Acknowledged_By__c,Negotiated_Partner_Agreement__c,Preferred_Language__c,FY_2021_NSFP_Allocation__c,MO_Verkada_Partner_Tier__c,Verkada_Partner_Score__c,Verkada_Partner_Tier__c,eventStartDate,CW_DR_Opps_In_Look_Back_Period__c,CW_Opps_In_Look_Back_Period__c,DR_Acceptance_Rate_In_Look_Back_Period__c,DR_Pipeline_In_Look_Back_Period__c,Total_Bookings_In_Look_Back_Period__c,Total_DR_Bookings_In_Look_Back_Period__c,of_Submitted_DR_In_Look_Back_Period__c,Current_Sequencer_ID__c,FY_2021_Nonprofit_Security_Grant_Program__c,School_Type__c,Educational_Account_Type__c,Clean_Verkada_Partner_Tier__c,zoom_app__IsCreatedByZoomApp__c,of_Accepted_Deal_Regs__c,Verkada_Alarms_Specialist__c,VAS_Training__c,VAS_Date__c,linkedinReferralContact,linkedinOtherComments,ContactUpdateFlag__c,AB841_Funding__c,Dialpad__Powerdialer_Assigned_List__c,Dialpad__Powerdialer_Dialed_List__c,Dialpad__Powerdialer_Last_Dialed_via__c,Dialpad__TotalNumberOfTimesDialed__c,Dialpad__Powerdialer_Assigned_List__c_account,Dialpad__Powerdialer_Dialed_List__c_account,Dialpad__Powerdialer_Last_Dialed_via__c_account,Dialpad__TotalNumberOfTimesDialed__c_account,Clean_HQ_County__c,Holdover_Period__c,One_year_holdover__c,Channel_Prospecting_Stage__c,Account_Name_Formula__c,No_Longer_At_Company__c,Id_of_Contact_at_New_Job__c,Customer_Referrer_Name__c,Customer_Referrer_Email__c,Customer_Referrer_Address__c,Lead_Gen_Referrer_Name__c,Person_Attribution_Date_PAD__c,Person_Attribution_Mechanism_PAM__c,Person_Website_Referrer_PWR__c,Lead_Gen_Referrer_Email__c,Lead_Gen_Referrer_Address__c,talkdesk__Count__c,HEER_III_Funding__c,jobchangecontacttype,jobchangecontactid,jobchangeaccountid,Employment_Start_Date__c,Employment_End_Date__c,VCE_Training_Location__c,VCE_Shipping_Address__c,VCE_Dietary_Restrictions__c,VCE_Covid_Agreement__c,Zerobounce_Last_Validated__c,Towerdata_Last_Validated__c,Mailgun_Last_Validated__c,Person_Referral__c,VCE_Shirt_Size__c,Predicted_Initial_End_Customer_Contact__c,Number_of_Open_Guest_Accessory_Trials__c,Credit_Card_Override__c,Purchased_Alarms__c,Purchased_Guest__c,amazonClaimCode,Out_of_Business__c,Tribal_Account__c,First_Touchpoint_Date__c,Last_Touchpoint_Date__c,Days_Since_Last_Touchpoint__c,Lead_Flow_Bucket__c,Account_Clean_HQ_Country__c,First_Opportunity_Created_Date__c,Account_Flow_Bucket__c,Account_Decision_Buying_Process__c,Account_Decision_Buying_Process_Override__c,Marketing_Strategy__c,Global_ENT__c,Number_of_Deal_Regs_Submitted_Last_90__c,SSIP_Budget__c,SSBA_Allocation__c,Referral_Detection_Method__c,ChannelProgramName,ChannelProgramLevelName,Alarm_Addendum_Required__c,Reason_for_Owner_Change__c,Incoming_New_Owner_Formula__c,Incoming_New_ENT_Territory_Owner__c,Incoming_New_ENT_Owner__c,Ultimate_ParentId__c,Staging_ParentId_Field_Value__c,Data_Review_Results__c,Customer_of_Product_Families__c,Customer_of_Product_Families__c_contact,GAM__c,Staging_Owner__c,Staging_Territory_Key__c
    """

    singer.write_schema(stream["tap_stream_id"], stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    start_date = bookmarks.get_bookmark(state, stream["tap_stream_id"], replication_key)
    params = {"batchSize": 300, "fields": test}
    
    # https://engage-ab.marketo.com/?munchkinId=739-BIK-855#/classic/ST13702A1LA1
    static_supplement_lead_list = 13702
    endpoint = f"rest/v1/list/{static_supplement_lead_list}/leads.json"
    
    # Paginated requests use paging tokens for retrieving the next page
    # of results. These tokens are stored in the state for resuming
    # syncs. If a paging token exists in state, use it.
    next_page_token = bookmarks.get_bookmark(state, stream["tap_stream_id"], "next_page_token")
    if next_page_token:
        params["nextPageToken"] = next_page_token

    # Keep querying pages of data until no next page token.
    record_count = 0
    job_started = pendulum.utcnow().isoformat()
    while True:
        data = client.request("GET", endpoint, endpoint_name=stream["tap_stream_id"], params=params)
        

        time_extracted = utils.now()

        # Each row just needs the values formatted. If the record is
        # newer than the original start date, stream the record. Finally,
        # update the bookmark if newer than the existing bookmark.
        for row in data["result"]:
            record = format_values(stream, row)
            if record[replication_key] >= start_date:
                record_count += 1

                singer.write_record(stream["tap_stream_id"], record, time_extracted=time_extracted)

        # No next page, results are exhausted.
        if "nextPageToken" not in data:
            break

        # Store the next page token in state and continue.
        params["nextPageToken"] = data["nextPageToken"]
        state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "next_page_token", data["nextPageToken"])
        singer.write_state(state)

    # Once all results are exhausted, unset the next page token bookmark
    # so the subsequent sync starts from the beginning.
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "next_page_token", None)
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], replication_key, job_started)
    singer.write_state(state)
    return state, record_count

def sync_activities_paginated(client, state, stream, activity_id):
    # https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Activities/getLeadActivitiesUsingGET
    # http://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Static_Lists/getListsUsingGET
    #
    # Activity Data are paginated with a max return of 300
    # items per page.
    replication_key = determine_replication_key(stream['tap_stream_id'])

    singer.write_schema(stream["tap_stream_id"], stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    start_date = bookmarks.get_bookmark(state, stream["tap_stream_id"], replication_key)
    params = {"batchSize": 300, "activityTypeIds": activity_id}
    endpoint = "rest/v1/activities.json"

    # Paginated requests use paging tokens for retrieving the next page
    # of results. These tokens are stored in the state for resuming
    # syncs. If a paging token exists in state, use it.
    next_page_token = bookmarks.get_bookmark(state, stream["tap_stream_id"], "next_page_token")
    if next_page_token:
        params["nextPageToken"] = next_page_token
    else:
        params["nextPageToken"] =  client.get_paging_token(start_date)  

    # Keep querying pages of data until no next page token.
    record_count = 0
    job_started = pendulum.utcnow().isoformat()
    while True:
        data = client.request("GET", endpoint, endpoint_name=stream["tap_stream_id"], params=params)
        time_extracted = utils.now()

        # Each row just needs the values formatted. If the record is
        # newer than the original start date, stream the record. Finally,
        # update the bookmark if newer than the existing bookmark.
        if data.get("result") != None:
            for row in data["result"]:
                row = flatten_activity(row, stream)
                record = format_values(stream, row)
                if record[replication_key] >= start_date:
                    record_count += 1

                    singer.write_record(stream["tap_stream_id"], record, time_extracted=time_extracted)

            # Store the next page token in state and continue.
            params["nextPageToken"] = data["nextPageToken"]
            state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "next_page_token", data["nextPageToken"])
            singer.write_state(state)
        else: 
            # No next page, results are exhausted.
            break

    # Once all results are exhausted, unset the next page token bookmark
    # so the subsequent sync starts from the beginning.
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "next_page_token", None)
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], replication_key, job_started)
    singer.write_state(state)
    return state, record_count

def sync_paginated(client, state, stream):
    # http://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Campaigns/getCampaignsUsingGET
    # http://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Static_Lists/getListsUsingGET
    #
    # Campaigns and Static Lists are paginated with a max return of 300
    # items per page. There are no filters that can be used to only
    # return updated records.
    replication_key = determine_replication_key(stream['tap_stream_id'])

    singer.write_schema(stream["tap_stream_id"], stream["schema"], stream["key_properties"], bookmark_properties=[replication_key])
    start_date = bookmarks.get_bookmark(state, stream["tap_stream_id"], replication_key)
    params = {"batchSize": 300}
    endpoint = "rest/v1/{}.json".format(stream["tap_stream_id"])

    # Paginated requests use paging tokens for retrieving the next page
    # of results. These tokens are stored in the state for resuming
    # syncs. If a paging token exists in state, use it.
    next_page_token = bookmarks.get_bookmark(state, stream["tap_stream_id"], "next_page_token")
    if next_page_token:
        params["nextPageToken"] = next_page_token

    # Keep querying pages of data until no next page token.
    record_count = 0
    job_started = pendulum.utcnow().isoformat()
    while True:
        data = client.request("GET", endpoint, endpoint_name=stream["tap_stream_id"], params=params)
        time_extracted = utils.now()

        # Each row just needs the values formatted. If the record is
        # newer than the original start date, stream the record. Finally,
        # update the bookmark if newer than the existing bookmark.
        for row in data["result"]:
            record = format_values(stream, row)
            if record[replication_key] >= start_date:
                record_count += 1

                singer.write_record(stream["tap_stream_id"], record, time_extracted=time_extracted)

        # No next page, results are exhausted.
        if "nextPageToken" not in data:
            break

        # Store the next page token in state and continue.
        params["nextPageToken"] = data["nextPageToken"]
        state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "next_page_token", data["nextPageToken"])
        singer.write_state(state)

    # Once all results are exhausted, unset the next page token bookmark
    # so the subsequent sync starts from the beginning.
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], "next_page_token", None)
    state = bookmarks.write_bookmark(state, stream["tap_stream_id"], replication_key, job_started)
    singer.write_state(state)
    return state, record_count


def sync_activity_types(client, state, stream):
    # http://developers.marketo.com/rest-api/lead-database/activities/#describe
    #
    # Activity types aren't even paginated. Grab all the results in one
    # request, format the values, and output them.

    singer.write_schema("activity_types", stream["schema"], stream["key_properties"])
    endpoint = "rest/v1/activities/types.json"
    data = client.request("GET", endpoint, endpoint_name="activity_types")
    record_count = 0

    time_extracted = utils.now()

    for row in data["result"]:
        record = format_values(stream, row)
        record_count += 1

        singer.write_record("activity_types", record, time_extracted=time_extracted)

    return state, record_count


def sync(client, catalog, config, state):
    starting_stream = bookmarks.get_currently_syncing(state)
    if starting_stream:
        singer.log_info("Resuming sync from %s", starting_stream)
    else:
        singer.log_info("Starting sync")

    for stream in catalog["streams"]:
        # Skip unselected streams.
        mdata = metadata.to_map(stream['metadata'])

        if not metadata.get(mdata, (), 'selected'):
            singer.log_info("%s: not selected", stream["tap_stream_id"])
            continue

        # Skip streams that have already be synced when resuming.
        if starting_stream and stream["tap_stream_id"] != starting_stream:
            singer.log_info("%s: already synced", stream["tap_stream_id"])
            continue

        singer.log_info("%s: starting sync", stream["tap_stream_id"])

        # Now that we've started, there's no more "starting stream". Set
        # the current stream to resume on next run.
        starting_stream = None
        state = bookmarks.set_currently_syncing(state, stream["tap_stream_id"])
        singer.write_state(state)

        # Sync stream based on type.
        if stream["tap_stream_id"] == "activity_types":
            state, record_count = sync_activity_types(client, state, stream)
        elif stream["tap_stream_id"] == "leads":
            state, record_count = sync_leads_paginated(client, state, stream)
        elif stream["tap_stream_id"] == "activities_send_email":
            state, record_count = sync_activities_paginated(client, state, stream, 6)
        elif stream["tap_stream_id"] == "activities_fill_out_form":
            state, record_count = sync_activities_paginated(client, state, stream, 2)
            
        # elif stream["tap_stream_id"].startswith("activities_"):
        #     state, record_count = sync_activities(client, state, stream, config)
        elif stream["tap_stream_id"] in ["campaigns", "lists"]:
            state, record_count = sync_paginated(client, state, stream)
        elif stream["tap_stream_id"] == "programs":
            state, record_count = sync_programs(client, state, stream)
        else:
            raise Exception("Stream %s not implemented" % stream["tap_stream_id"])

        # Emit metric for record count.
        counter = singer.metrics.record_counter(stream["tap_stream_id"])
        counter.value = record_count
        counter._pop()  # pylint: disable=protected-access

        # Unset current stream.
        state = bookmarks.set_currently_syncing(state, None)
        singer.write_state(state)
        singer.log_info("%s: finished sync", stream["tap_stream_id"])

    # If Corona is not supported, log a warning near the end of the tap
    # log with instructions on how to get Corona supported.
    singer.log_info("Finished sync.")
    if not client.use_corona:
        singer.log_warning(NO_CORONA_WARNING)
