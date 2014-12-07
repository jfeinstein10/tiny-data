import re

def map(file_line):

    # CONSTANTS
    SENTBREAK = '_s'

    # count n-grams
    # and categorize by review status for review
    words = [SENTBREAK, SENTBREAK, SENTBREAK]  # [0] = curWord, [1] = prevWord, [2] = prevPrevWord
    line = file_line.strip()
    else:
        # Extract verse
        verse_line = line.split(' ',1)
        if (len(verse_line)==2):
            # process line
            line = verse_line[1]
            tokens = word_tokenize(line)
            length = len(tokens)
            while (i<length):
                # Shift word window
                words[2] = words[1]
                words[1] = words[0]
                words[0] = tokens[i]
                ngram = words[0]
                j=0
                for j in range(3):
                    if (j!=0): ngram = words[j] + SPACE + ngram
                    incr_dict_cnt(darry[j], ngram)
                # other updates
                i+=1
    # update curline
    lineNum+=1
    curLine = f.readline()
# end with sentence break
words[2] = words[1]
words[1] = words[0]
words[0] = SENTBREAK
ngram = words[0]
j=0
for j in range(3):
    if (j!=0): ngram = words[j] + SPACE + ngram
    incr_dict_cnt(darry[j], ngram)



def word_tokenize(line):
    split_arr = re.split('(\s*\W*\s*)', line)
    tokens = []
    for split in split_arr:
        stripped_split = split.strip()
        if (not stripped_split==''):  tokens.append(stripped_split)
    return tokens