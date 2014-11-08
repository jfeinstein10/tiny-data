'''
Created on Nov 4, 2014

@author: Brian
'''



if __name__ == '__main__':
    s = ''
    with open('compile_orig.pyc','r') as fr:
        for line in fr:
            s += line
            
    with open('compile.pyc','w') as fw:
        fw.write(s.strip())
            
    from brian_test.compile import *
    mymap()