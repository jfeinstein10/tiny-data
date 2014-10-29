from master.threads import MasterServer


def main():
    m_server = MasterServer()
    m_server.start()
    m_server.join()


if __name__ == '__main__':
    main()