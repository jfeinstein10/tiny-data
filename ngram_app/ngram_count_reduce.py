from collections import defaultdict

def reduce_fn(key, val_list):
    count = sum(val_list)
    return count