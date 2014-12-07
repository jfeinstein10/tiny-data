from threads import *
import time
import timeit



def upload():
    client = ClientThread()
    client.send_upload('/kjbible_small.txt', 'kjbible_small.txt', 1000)
    client.start()
    client.join()



def mapreduce():
    client = ClientThread()
    client.send_map_reduce('/kjbible_small.txt', '/kjbible_small_r.txt', 'ngram_count_map.py', 'ngram_count_reduce.py', 'ngram_count_combine.py')
    client.start()
    client.join()



def main():
    print timeit.timeit(upload, number=1)
    print timeit.timeit(mapreduce, number=1)



if __name__ == '__main__':
    main()