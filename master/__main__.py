from master.threads import ClientThread, FollowerThread

cThread = ClientThread()
fThread = FollowerThread()

cThread.start()
fThread.start()

cThread.join()
fThread.join()