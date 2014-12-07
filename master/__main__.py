from threads import MasterServer, FollowerAcceptor


def main():
    follower_acceptor = FollowerAcceptor()
    follower_acceptor.start()
    master_server = MasterServer()
    master_server.start()
    master_threads = [master_server, follower_acceptor]
    for thread in master_threads:
        thread.join()
    return master_threads



if __name__ == '__main__':
    master_threads = main()
    for thread in master_threads:
        thread.join()
