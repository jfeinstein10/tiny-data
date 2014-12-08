

def map_fn(line):
    split = line.split(',')
    ab = split[7]
    hits = split[9:13]
    if not ab.isdigit() or False in map(lambda x: x.isdigit(), hits):
        return []
    ab = float(ab)
    if ab == 0:
        return []
    hits = map(float, hits)
    singles = hits[0] - hits[1] - hits[2] - hits[3]
    return [(split[0], (singles + 2*hits[1] + 3*hits[2] + 4*hits[3]) / ab)]


def reduce_fn(key, values):
    return max(values)