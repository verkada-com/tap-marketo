import argparse
import collections
import datetime
import functools
import json
import os
import time

DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"


def strptime(dt):
    return datetime.datetime.strptime(dt, DATETIME_FMT)


def strftime(dt):
    return dt.strftime(DATETIME_FMT)


def ratelimit(limit, every):
    def limitdecorator(fn):
        times = collections.deque()

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if len(times) >= limit:
                t0 = times.pop()
                t = time.time()
                sleep_time = period - (t - t0)
                if sleep_time > 0:
                    logger.info("t0 = {}, t = {}, sleeping for {} seconds".format(t0, t, sleep_time))
                    time.sleep(sleep_time)

            times.appendleft(time.time())
            return fn(*args, **kwargs)

        return wrapper

    return limitdecorator


def chunk(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_json(path):
    with open(path) as f:
        return json.load(f)


def load_schema(entity):
    return load_json(get_abs_path("schemas/{}.json".format(entity)))


def update_state(state, entity, dt):
    if dt is None:
        return

    if isinstance(dt, datetime.datetime):
        dt = strftime(dt)

    if entity not in state:
        state[entity] = dt

    if dt >= state[entity]:
        state[entity] = dt


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    return parser.parse_args()


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))