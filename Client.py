__author__ = 'Sestren'
import threading
import socket
import pickle
import os

def client(id,master):
    #print('CREATED CLIENT',id)
    pid = open('pidlog', 'a')
    pid.write(str(os.getpid())+'\n')
    pid.close()

    compkg=[id,'null','null','null',[]]

    comth = threading.Thread(target=commthread,args=[compkg])
    comth.start()
    while True:
        if compkg[1] != 'null':
            master.send(compkg[1])
            break

    # Main Loop
    while True:
        test = master.poll()
        if test:
            command = master.recv()
            if command[0] == 0:  # Retire server
                pass
            elif command[0] == 1:  # Break connection
                compkg[3] = command[1]
                while True:
                    if compkg[3] == 'null':
                        master.send(0)
                        break
            elif command[0] == 2:  # Create/Restore connection
                compkg[2] = command[1]
                while True:
                    if compkg[2] == 'null':
                        master.send(0)
                        break
            elif command[0] == 7:
                compkg[4] = ['put',command[1],command[2]]
                while True:
                    if not compkg[4]:
                        master.send(0)
                        break
            elif command[0] == 8:
                compkg[4] = ['get',command[1]]
                while True:
                    if not compkg[4]:
                        master.send(0)
                        break
            elif command[0] == 9:
                compkg[4] = ['delete',command[1]]
                while True:
                    if not compkg[4]:
                        master.send(0)
                        break
            else:
                print('SERVER',id,'UNKNOWN COMMAND',command)


def commthread(compkg):
    #print('CREATED COMMTHREAD',compkg[0])
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost',0))
    sock.listen(5)
    compkg[1] = sock.getsockname()[1]
    connections = []
    last_timestamp = ['null','null']

    # Main Loop
    while True:

        if compkg[2] != 'null':
            sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock2.connect(('localhost',compkg[2]))
            sock2.send(b'\x01')
            s, addr = sock.accept()
            kind = s.recv(1)
            if kind == b'\x00':
                tid = 's'
            elif kind == b'\x01':
                tid = 'c'
                print('CLIENT CONNECTED CLIENT - IMPROPER BEHAVIOR')
            else:
                tid = 'u'
                print('UNKNOWN TYPE RECEIVED')
            if tid == 's':
                length = s.recv(2)
                s.recv(int.from_bytes(length,'big'))  # Throw the id away, don't need it
            connections.append([s,sock2,tid])  # incoming/outgoing/type/anti-entropy state/internal id,vector
            compkg[2] = 'null'
            #print('COMMTHREAD',compkg[0],'ESTABLISHED CONNECTION')
            #print(compkg[0],'ALL CONNECTIONS',connections)

        if compkg[3] != 'null':
            for peer in connections:
                if peer[1].getpeername()[1] == compkg[3]:
                    peer[0].close()
                    peer[1].close()
                    connections.remove(peer)
                    break
            compkg[3] = 'null'
            #print('COMMTHREAD',compkg[0],'CLOSED CONNECTION')

        if compkg[4]:
            if compkg[4][0] == 'put':
                tag = b'\x00\x02'
            elif compkg[4][0] == 'get':
                tag = b'\x00\x03'
            elif compkg[4][0] == 'delete':
                tag = b'\x00\x04'
            else:
                tag = b'\xff'
            compkg[4].append(last_timestamp[0])
            compkg[4].append(last_timestamp[1])
            messagecontent = tag+pickle.dumps(compkg[4])
            length = len(messagecontent).to_bytes(2,'big')
            command = length + messagecontent
            connections[0][1].send(command)
            #print(compkg[0],'SENT COMMAND, GETTING RESPONSE')
            try:
                leng = connections[0][0].recv(2)
                #print('RESPONSE LENGTH',leng)
                if leng == b'':
                    connections[0][0].close()
                    connections[0][1].close()
                    connections.remove(connections[0])
                    #print('CONNECTION CLOSED',compkg[0])
                else:  # Grab Message
                    message = connections[0][0].recv(int.from_bytes(leng,'big'))
            except ConnectionError:
                connections[0][0].close()
                connections[0][1].close()
                connections.remove(connections[0])
                #print('CONNECTION FAILED',compkg[0])
            else:
                lastime = pickle.loads(message)
                if lastime[0] == 'ERR_DEP':
                    if compkg[4][0] == 'get':
                        print('ERR_DEP')
                else:
                    last_timestamp[0] = lastime[0]
                    last_timestamp[1] = lastime[1]
                    if compkg[4][0] == 'get':
                        if lastime[2] == 'ERR_KEY':
                            print('ERR_KEY')
                        else:
                            print(str(compkg[4][1])+':'+str(lastime[2]))
            compkg[4] = []