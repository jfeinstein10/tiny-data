import re


def map_fn(line):
    words = re.findall(r"[\w']+", line)
    return [(word.strip().lower(), 1) for word in words]


def reduce_fn(key, values):
    return sum(values)