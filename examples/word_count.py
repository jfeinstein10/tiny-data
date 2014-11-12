

def map_fn(line):
    words = line.split(',')
    return [(word, 1) for word in words]


def reduce_fn(key, values):
    return sum(values)