from master.threads import MasterServer


def main():
    m_server = MasterServer()
    m_server.start()
    return m_server


if __name__ == '__main__':
    main()