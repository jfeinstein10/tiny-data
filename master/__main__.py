from master.threads import MasterServer


def main():
    mServer = MasterServer()
    mServer.start()
    mServer.join()


if __name__ == '__main__':
    main()