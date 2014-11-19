from client.threads import *



def main():
    client = ClientThread()
    client.send_upload('kjbible.txt', 'kjbible.txt', 1000)
    client.send_map_reduce('kjbible.txt', 'kjbible_results.txt', job_contents)  # agree on this



if __name__ == '__main__':
    main()