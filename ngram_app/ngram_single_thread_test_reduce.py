import sys
import ngram_count_map as map_mod
import ngram_count_combine as combine_mod
import ngram_count_reduce as reduce_mod



def main():

    filename = sys.argv[1]
    base_filename = filename.split('.')[0]

    # Perform map and collect results in dictionary
    result_dict_1 = {}
    result_dict_2 = {}
    result_list = []
    counts_recorded = []
    line_num = 0

    # MAP AND COMBINE
    with open(filename, 'r') as f:
        for line in f:
            line_num += 1
            if (line_num%2 == 0):  result_dict = result_dict_1
            else:  result_dict = result_dict_2
            pairs, counts = map_mod.map_fn(line)
            for key, value in pairs:
                if result_dict.has_key(key):  result_dict[key].append(value)
                else: result_dict[key] = [value]
            count_len = len(counts_recorded)
            for i in range(len(counts)):
                if (i<count_len): counts_recorded[i] += counts[i]
                else:  counts_recorded.append(counts[i])
    for key in result_dict_1:
        result_list.append((key, combine_mod.combine_fn(result_dict_1[key])))
    for key in result_dict_2:
        result_list.append((key, combine_mod.combine_fn(result_dict_2[key])))

    # REDUCE
    result_list, counts = reduce_mod.reduce_fn(result_list)

    # Sort result list
    result_list.sort(sort_key_vals)
    with open(base_filename + '_results.txt', 'w') as f:
        for key, val in result_list:
            f.write(key + ' ' + str(val) + '\n')
    print('Counts:  ' + str(counts_recorded))



def sort_key_vals(kv1, kv2):
    if (kv1[0]<kv2[0]): return -1
    if (kv1[0]>kv2[0]): return 1
    return 0



if __name__ == '__main__':
    main()
