from collections import defaultdict

def reduce_fn(key_val_list):
    ngram_dictionary = {}
    # Collect values in dictionary
    for (key, val) in key_val_list:
        if (ngram_dictionary.has_key(key)):  ngram_dictionary[key] += val
        else:  ngram_dictionary[key] = val
    # Output to list
    key_val_list_out = []
    for key in ngram_dictionary:
        key_val_list_out.append((key, ngram_dictionary[key]))
    return key_val_list_out, []