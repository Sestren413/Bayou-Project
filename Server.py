__author__ = 'Sestren'
import threading
import socket
import select
import pickle
import os

def server(id,master,primary):
    #print('CREATED SERVER',id)
    pid = open('pidlog', 'a')
    pid.write(str(os.getpid())+'\n')
    pid.close()

    #       0     1        2        3        4          5            6             7         8       9
    #       id  myport  yourport  aeflag internal_id writelog stabilize period breakport retiring primary
    compkg=[id, 'null',  'null',   True,   'null',      [],          0,         'null',      0,   primary]

    if compkg[9]:
        compkg[4] = b'\x00'

    version_vector = {}  # Yeah its a Dict so I can look up entries based on internal id
    version_vector['csn'] = -1

    loglock = threading.Lock()
    comth = threading.Thread(target=commthread,args=(compkg,loglock,version_vector))
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
                compkg[8] = 1
                while True:
                    if compkg[8] == 3:
                        master.send(0)
                        break
            elif command[0] == 1:  # Break connection
                compkg[7] = command[1]
                while True:
                    if compkg[7] == 'null':
                        master.send(0)
                        break
            elif command[0] == 2:  # Create/Restore connection
                compkg[2] = command[1]
                while True:
                    if compkg[2] == 'null':
                        master.send(0)
                        break
            elif command[0] == 3:  # Pause anti-entropy
                compkg[3] = False
                master.send(0)
            elif command[0] == 4:  # Start anti-entropy
                compkg[3] = True
                master.send(0)
            elif command[0] == 5:  # Stabilize
                compkg[6] = command[1]*2
                while True:
                    if compkg[6] == 0:
                        master.send(0)
                        break
            elif command[0] == 6:  # Print Log
                #print('SERVER',id,'GOT LOG REQUEST')
                loglock.acquire()
                #compkg[5].sort(key=lambda writelog: writelog[:1])
                litelog = []
                for w in compkg[5]:
                    if w[3] == 'put' or w[3] == 'delete':
                        litelog.append(w)
                #print(litelog)
                for l in litelog:
                    if l[3] == 'put':
                        optype = 'PUT'
                        opvalue = str(l[4])+', '+str(l[5])
                    else:
                        optype = 'DELETE'
                        opvalue = str(l[4])
                    if l[0] != float('inf'):
                        stablebool = 'TRUE'
                    else:
                        stablebool = 'FALSE'
                    string = optype+':('+opvalue+'):'+stablebool
                    print(string)
                loglock.release()
                master.send(0)
            else:
                print('SERVER',id,'UNKNOWN COMMAND',command)

        if compkg[9]:  # Stabilize writes
            for w in compkg[5]:
                if w[0] == float('inf'):
                    #print(id,'STABILIZING',w)
                    w[0] = version_vector['csn']+1
                    version_vector['csn'] += 1


def commthread(compkg,loglock,version_vector):
    #print('CREATED COMMTHREAD',compkg[0])

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost',0))
    sock.listen(5)
    compkg[1] = sock.getsockname()[1]

    connections = []

    current_timestamp = 0

    sent_create = compkg[9]  # Sent create write to some other server
    if compkg[9]:
        version_vector[b'\x00'] = 0

    # Main Loop
    while True:

        if compkg[2] != 'null':
            sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock2.connect(('localhost',compkg[2]))
            sock2.send(b'\x00')
            s, addr = sock.accept()
            kind = s.recv(1)
            if kind == b'\x00':
                tid = 's'
            elif kind == b'\x01':
                tid = 'c'
            else:
                tid = 'u'
                print('UNKNOWN TYPE RECEIVED')
            if compkg[4] != 'null':
                iid = compkg[4]
            else:
                iid = b''
            length = len(iid).to_bytes(2,'big')
            sock2.send(length+iid)
            if tid == 's':
                length = s.recv(2)
                iiid = s.recv(int.from_bytes(length,'big'))
            else:
                iiid = 'null'
            if iiid == b'':
                iiid = 'null'
            connections.append([s,sock2,tid,0,iiid,{}])  # incoming/outgoing/type/anti-entropy state/internal id,vector
            compkg[2] = 'null'
            #print('COMMTHREAD',compkg[0],'ESTABLISHED CONNECTION')
            #print(compkg[0],'ALL CONNECTIONS',connections[:][:])

        if compkg[7] != 'null':
            for peer in connections:
                if peer[1].getpeername()[1] == compkg[7]:
                    connections.remove(peer)
                    peer[0].close()
                    peer[1].close()
                    #print('COMMTHREAD',compkg[0],'CLOSED CONNECTION')
                    #print(compkg[0],'ALL CONNECTIONS',connections)
                    break
            else:
                pass
                #print('COMMTHREAD',compkg[0],'DOES NOT HAVE CONNECTION TO CLOSE')
            compkg[7] = 'null'

        # RETIREMENT STEPS
        if compkg[8] == 1:
            loglock.acquire()
            compkg[5].append([float('inf'),current_timestamp,compkg[4],'retire',compkg[9]])
            loglock.release()
            current_timestamp += 1
            version_vector[compkg[4]] = current_timestamp
            compkg[8] = 2

        # SEND OUT CREATE WRITE AND GET ID
        if not sent_create:  # Has not sent create write
            if connections:  # Is connected to at lest one other something
                for peer in connections:
                    if peer[2] == 's':  # The something is a server and not a client or unknown
                        messagecontent = b'\x00\x00'  # WRITE - CREATE
                        length = len(messagecontent).to_bytes(2,'big')
                        msg = length + messagecontent
                        peer[1].send(msg)
                        #print(compkg[0],'SENT CREATE WRITE TO',peer[4])
                        length = peer[0].recv(2)
                        compkg[4] = peer[0].recv(int.from_bytes(length,'big'))
                        #print(compkg[0],'INTERNAL ID OF',compkg[4])
                        current_timestamp = int.from_bytes(compkg[4][:1],'big')+1
                        version_vector[compkg[4]] = current_timestamp
                        sent_create = True
                        iid = b'\x02'+compkg[4]
                        length = len(iid).to_bytes(2,'big')
                        liid = length + iid
                        for peer in connections:
                            peer[1].send(liid)
                        break

        # ANTI-ENTROPY OUTGOING
        if sent_create:
            for peer in connections:
                if peer[4] != 'null' and peer[3] == 0 and compkg[3]:  # A server that needs to send its part of the vector
                    #print(compkg[0],'SENT AE VECTOR REQUEST TO',peer[4])
                    messagecontent = b'\x01\x00'  # AE - VECTOR REQUEST
                    length = len(messagecontent).to_bytes(2,'big')
                    msg = length + messagecontent
                    peer[1].send(msg)
                    peer[3] = 1
                    if compkg[6] > 0:  # Outgoing AE request counter
                        compkg[6] -= 1

        # ANTI-ENTROPY BYPASS FOR STABILIZATION WHEN SERVER IS ENTIRELY ISOLATED
        if sent_create:
            for peer in connections:
                if peer[2] == 's':
                    break
            else:  # No servers found
                compkg[6] = 0

        # RECEIVE MESSAGES
        incoming = []
        for peer in connections:
            incoming.append(peer[0])
        if incoming and compkg[8] < 3:
            ready_to_read, b, c = select.select(incoming,[],[],0.01)
            for rtrsock in ready_to_read:
                for con in connections:
                    if con[0] == rtrsock:
                        peer = con
                try:
                    leng = rtrsock.recv(2)
                    if leng == b'':
                        connections.remove(peer)
                        peer[0].close()
                        peer[1].close()
                        #print('CONNECTION CLOSED',compkg[0])
                    else:  # Grab Message
                        message = rtrsock.recv(int.from_bytes(leng,'big'))
                        #print(compkg[0],'RECVD MESSAGE',message)
                except ConnectionError:
                    connections.remove(peer)
                    peer[0].close()
                    peer[1].close()
                    #print('CONNECTION FAILED',compkg[0])
                else:  # Handle Message if it needs immediate action before being sent on
                    if message[:1] == b'\x00' and compkg[8] == 0:  # is a WRITE of some sort
                        loglock.acquire()
                        if message[1:2] == b'\x00':  # is a CREATE
                            #print('SERVER GOT A CREATE')
                            a = current_timestamp.to_bytes(1,'big')
                            b = a + compkg[4]
                            length = len(b).to_bytes(2,'big')
                            peer[1].send(length+b)
                            compkg[5].append([float('inf'),current_timestamp,compkg[4],'create'])
                            current_timestamp += 1
                            version_vector[compkg[4]] = current_timestamp
                        elif message[1:2] == b'\x01':  # RETIRE (shouldn't get this command from another server)
                            pass
                        elif message[1:2] == b'\x02':  # PUT
                            command = pickle.loads(message[2:])
                            for w in compkg[5]:
                                if command[3:5] == w[1:3] or command[3:5] == ['null','null']:
                                    neww = [float('inf'),current_timestamp,compkg[4],'put']
                                    neww.append(command[1])
                                    neww.append(command[2])
                                    compkg[5].append(neww)
                                    messagecontent = pickle.dumps([current_timestamp,compkg[4]])
                                    length = len(messagecontent).to_bytes(2,'big')
                                    lastime = length + messagecontent
                                    peer[1].send(lastime)
                                    version_vector[compkg[4]] = current_timestamp
                                    current_timestamp += 1
                                    break
                            else:  # Dependency error
                                messagecontent = pickle.dumps(['ERR_DEP'])
                                length = len(messagecontent).to_bytes(2,'big')
                                lastime = length + messagecontent
                                peer[1].send(lastime)
                        elif message[1:2] == b'\x03':  # GET
                            command = pickle.loads(message[2:])
                            for w in compkg[5]:
                                if command[2:4] == w[1:3] or command[2:4] == ['null','null']:
                                    neww = [float('inf'),current_timestamp,compkg[4],'get']
                                    neww.append(command[1])
                                    compkg[5].append(neww)
                                    litelog = []
                                    for w in compkg[5]:
                                        if w[3] == 'put' or w[3] == 'delete':
                                            litelog.append(w)
                                    #print('LITELOG',litelog)
                                    playlist = {}
                                    for l in litelog:
                                        if l[3] == 'put':
                                            playlist[l[4]] = l[5]
                                        if l[3] == 'delete' and l[4] in playlist:
                                            del playlist[l[4]]
                                    if command[1] in playlist:
                                        get_return = playlist[command[1]]
                                    else:
                                        get_return = 'ERR_KEY'
                                    messagecontent = pickle.dumps([current_timestamp,compkg[4],get_return])
                                    length = len(messagecontent).to_bytes(2,'big')
                                    lastime = length + messagecontent
                                    peer[1].send(lastime)
                                    version_vector[compkg[4]] = current_timestamp
                                    current_timestamp += 1
                                    break
                            else:  # Dependency error
                                messagecontent = pickle.dumps(['ERR_DEP'])
                                length = len(messagecontent).to_bytes(2,'big')
                                lastime = length + messagecontent
                                peer[1].send(lastime)
                        elif message[1:2] == b'\x04':  # DELETE
                            command = pickle.loads(message[2:])
                            for w in compkg[5]:
                                if command[2:4] == w[1:3] or command[2:4] == ['null','null']:
                                    neww = [float('inf'),current_timestamp,compkg[4],'delete']
                                    neww.append(command[1])
                                    compkg[5].append(neww)
                                    messagecontent = pickle.dumps([current_timestamp,compkg[4]])
                                    length = len(messagecontent).to_bytes(2,'big')
                                    lastime = length + messagecontent
                                    peer[1].send(lastime)
                                    version_vector[compkg[4]] = current_timestamp
                                    current_timestamp += 1
                                    break
                            else:  # Dependency error
                                messagecontent = pickle.dumps(['ERR_DEP'])
                                length = len(messagecontent).to_bytes(2,'big')
                                lastime = length + messagecontent
                                peer[1].send(lastime)
                        else:
                            print('UNKNOWN WRITE MESSAGE TYPE',compkg[0])
                        loglock.release()
                    elif message[:1] == b'\x01':  # AE
                        if message[1:2] == b'\x00':  # is a VECTOR REQUEST
                            messagecontent = b'\x01\x01'+pickle.dumps(version_vector)
                            length = len(messagecontent).to_bytes(2,'big')
                            vectorcopy = length + messagecontent
                            peer[1].send(vectorcopy)
                        elif message[1:2] == b'\x01':  # is a VECTOR RETURN
                            rvector = pickle.loads(message[2:])
                            loglock.acquire()
                            compkg[5].sort(key=lambda writelog: writelog[:3])
                            loglock.release()
                            for w in compkg[5]:
                                if w[0] > rvector['csn']:
                                    if w[0] != float('inf'):  # if stable
                                        if w[2] in rvector:  # R knows of initial server
                                            if rvector[w[2]] < w[1]:  # R has never heard of this w
                                                messagecontent = b'\x01\x02'+pickle.dumps(w)
                                                length = len(messagecontent).to_bytes(2,'big')
                                                wcopy = length + messagecontent
                                                peer[1].send(wcopy)
                                            else:  # R has an unstable version of this w
                                                messagecontent = b'\x01\x03'+pickle.dumps([w[0],w[1],w[2]])
                                                length = len(messagecontent).to_bytes(2,'big')
                                                wstab = length + messagecontent
                                                peer[1].send(wstab)
                                        else:  # Always send - because of the csn, you know R does not have this
                                            messagecontent = b'\x01\x02'+pickle.dumps(w)
                                            length = len(messagecontent).to_bytes(2,'big')
                                            wcopy = length + messagecontent
                                            peer[1].send(wcopy)
                                    else:  # if tentative
                                        if w[2] in rvector:  # R knows of initial server
                                            if rvector[w[2]] < w[1]:  # R has never heard of this w
                                                messagecontent = b'\x01\x02'+pickle.dumps(w)
                                                length = len(messagecontent).to_bytes(2,'big')
                                                wcopy = length + messagecontent
                                                peer[1].send(wcopy)
                                        else:  # R does not know of this server -- ADD RETIREMENT HANDLING NotImplemented
                                            messagecontent = b'\x01\x02'+pickle.dumps(w)
                                            length = len(messagecontent).to_bytes(2,'big')
                                            wcopy = length + messagecontent
                                            peer[1].send(wcopy)
                                if w[3] == 'retire' and w[4] == True:
                                    w[4] == False
                            peer[3] = 0
                            if compkg[8] == 2:
                                #print(compkg[0],'READY TO TERMINATE')
                                compkg[8] = 3
                        elif message[1:2] == b'\x02':  # is a WRITE UPDATE
                            loglock.acquire()
                            neww = pickle.loads(message[2:])
                            version_vector[neww[2]] = neww[1]
                            if neww[0] < version_vector['csn']+1:  # got a repeat
                                pass
                            elif neww[0] == version_vector['csn']+1:  # got the next stable write
                                version_vector['csn'] += 1
                                for w in compkg[5]:
                                    if neww[1:3] == w[1:3]:
                                        w[0] = neww[0]
                                        break
                                else:
                                    if neww[3] == 'create':
                                        niid = neww[1].to_bytes(1,'big')+neww[2]
                                        #print('ADDING',niid,'TO VECTOR')
                                        version_vector[niid] = neww[1]
                                        #print(version_vector)
                                    elif neww[3] == 'retire':
                                        #print(compkg[0],'REMOVING',neww[2],'FROM VECTOR')
                                        del version_vector[neww[2]]
                                        #print(version_vector)
                                        if neww[4] == True:
                                            compkg[9] = True
                                            neww[4] = False
                                            #print(compkg[0],'IS NOW THE NEW PRIMARY')
                                    compkg[5].append(neww)
                            elif neww[0] == float('inf'):  # got an unstable write
                                for w in compkg[5]:
                                    if neww[1:3] == w[1:3]:
                                        break
                                else:
                                    if neww[3] == 'create':
                                        niid = neww[1].to_bytes(1,'big')+neww[2]
                                        #print('ADDING',niid,'TO VECTOR')
                                        version_vector[niid] = neww[1]
                                        #print(version_vector)
                                    elif neww[3] == 'retire':
                                        #print('REMOVING',neww[2],'FROM VECTOR')
                                        del version_vector[neww[2]]
                                        #print(version_vector)
                                        if neww[4] == True:
                                            compkg[9] = True
                                            neww[4] = False
                                            #print(compkg[0],'IS NOW THE NEW PRIMARY')
                                    compkg[5].append(neww)
                            else:  # somehow missed a stable update
                                print('MISSED A STABLE UPDATE',compkg[0],version_vector['csn'],neww)
                            if neww[1] >= current_timestamp:
                                current_timestamp = neww[1] + 1
                                version_vector[compkg[4]] = current_timestamp
                            loglock.release()
                        elif message[1:2] == b'\x03':  # stabilize existing write
                            timestamp = pickle.loads(message[2:])
                            loglock.acquire()
                            for w in compkg[5]:
                                if w[1] == timestamp[1] and w[2] == timestamp[2]:
                                    if timestamp[0] == version_vector['csn']+1:
                                        w[0] = timestamp[0]
                                        version_vector['csn'] += 1
                                    elif timestamp[0] < version_vector['csn']+1:  # Got a repeat
                                        pass
                                    else:
                                        print('MISSED A STABLE UPDATE (STAB)',compkg[0],version_vector['csn'],timestamp)
                                    break
                            else:
                                print('A WRITE STABILIZE COULDNT FIND THE INDICATED WRITE')
                            loglock.release()
                        else:
                            print('UNKNOWN AE MESSAGE TYPE',compkg[0])
                    elif message[:1] == b'\x02':  # INTERNAL ID
                        peer[4] = message[1:]
                        #print(compkg[0],'GOT NEW IID, UPDATED CONNECTIONS',connections)
                    else:
                        print('UNKNOWN MESSAGE TYPE',compkg[0],message)