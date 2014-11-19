import sys
import re

orig_file = sys.argv[1]
new_file = sys.argv[2]

with open(orig_file, 'r') as fr:
    with open(new_file, 'w') as fw:

        cur_sentence = ''

        for line in fr:
            if not 'Page' in line:      # Discard line if it contains 'Page'
                line_arr = re.split('(\s*{\d+:\d+}\s*)', line)
                for line_seg in line_arr:
                    line_seg_stripped = line_seg.strip()
                    match = re.match('\s*{\d+:\d+}\s*', line_seg_stripped)
                    if (match):
                        if (cur_sentence!=''): fw.write(cur_sentence + '\n')
                        cur_sentence = line_seg.strip()
                    elif (line_seg_stripped!=''): cur_sentence += (' ' + line_seg.strip())

        # Write last line to file
        if (cur_sentence!=''): fw.write(cur_sentence + '\n')