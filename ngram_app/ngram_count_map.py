import re

def map_fn(file_line):

    # CONSTANTS
    MAX_NGRAM_LENGTH = 3
    SENTBREAK = '_s'
    SPACE = ' '

    # Variables
    key_val_dictionary = {}
    key_val_list = []
    ngram_count = 0

    # count n-grams
    # and categorize by review status for review
    words = [SENTBREAK, SENTBREAK, SENTBREAK]  # [0] = curWord, [1] = prevWord, [2] = prevPrevWord
    line = file_line.strip()
    # Extract verse
    verse_line = line.split(' ',1)
    if (len(verse_line)==2):
        # process line
        line = verse_line[1]
        tokens = word_tokenize(line)
        for token in tokens:
            ngram_count += 1
            # Shift word window
            words[2] = words[1]
            words[1] = words[0]
            words[0] = token
            ngram = words[0]
            for j in range(MAX_NGRAM_LENGTH):
                if (j!=0): ngram = words[j] + SPACE + ngram
                if (key_val_dictionary.has_key(ngram)):  key_val_dictionary[ngram] += 1
                else:  key_val_dictionary[ngram] = 1
        # end with sentence break
        ngram_count += 1
        words[2] = words[1]
        words[1] = words[0]
        words[0] = SENTBREAK
        ngram = words[0]
        for j in range(MAX_NGRAM_LENGTH):
            if (j!=0): ngram = words[j] + SPACE + ngram
            if (key_val_dictionary.has_key(ngram)):  key_val_dictionary[ngram] += 1
            else:  key_val_dictionary[ngram] = 1

    for key in key_val_dictionary:
        key_val_list.append((key, key_val_dictionary[key]))

    return key_val_list, [ngram_count]



def word_tokenize(line):
    split_arr = re.split('(\s*[^\w\s]*\s*)', line)
    tokens = []
    for split in split_arr:
        stripped_split = split.strip()
        if (not stripped_split==''):  tokens.append(stripped_split)
    return tokens