__author__ = 'Sestren'

from multiprocessing import Process,Pipe
import fileinput
import time
import Server
import Client
import os


def joinServer(svs,sid,cls):
    if not svs:
        here, there = Pipe()
        prss = Process(target=Server.server,args=(sid,there,True))
        prss.start()
        time.sleep(.001)
        addr = here.recv()
        svs[sid] = [prss, here, addr]
    else:
        here, there = Pipe()
        prss = Process(target=Server.server,args=(sid,there,False))
        prss.start()
        time.sleep(.001)
        addr = here.recv()
        svs[sid] = [prss, here, addr]
        for id in svs:
            if id != sid:
                restoreConnection(sid,id,svs,cls)


def retireServer(sid,svs):
    svs[sid][1].send([0])
    svs[sid][1].recv()
    svs[sid][0].terminate()
    del svs[sid]
    #print(sid,'RETIRED')


def joinClient(cid,sid,svs,cls):
    here, there = Pipe()
    prss = Process(target=Client.client,args=(cid,there))
    prss.start()
    time.sleep(.001)
    addr = here.recv()
    cls[cid] = [prss, here, addr]
    restoreConnection(sid,cid,svs,cls)


def breakConnection(id1,id2,svs,cls):
    #print('DISCONNECTING',id1,'AND',id2)
    for s in svs:
        if id1 == s:
            con1 = svs[s]
            break
    else:
        for c in cls:
            if id1 == c:
                con1 = cls[c]
                break
        else:
            print(id1,'DOES NOT APPEAR TO BE A VALID ID')
            return
    for s in svs:
        if id2 == s:
            con2 = svs[s]
            break
    else:
        for c in cls:
            if id2 == c:
                con2 = cls[c]
                break
        else:
            print(id2,'DOES NOT APPEAR TO BE A VALID ID')
            return
    con1[1].send([1,con2[2]])
    con2[1].send([1,con1[2]])
    con1[1].recv()
    con2[1].recv()


def restoreConnection(id1,id2,svs,cls):
    #print('CONNECTING',id1,'AND',id2)
    for s in svs:
        if id1 == s:
            con1 = svs[s]
            break
    else:
        for c in cls:
            if id1 == c:
                con1 = cls[c]
                break
        else:
            print(id1,'DOES NOT APPEAR TO BE A VALID ID')
            return
    for s in svs:
        if id2 == s:
            con2 = svs[s]
            break
    else:
        for c in cls:
            if id2 == c:
                con2 = cls[c]
                break
        else:
            print(id2,'DOES NOT APPEAR TO BE A VALID ID')
            return
    con1[1].send([2,con2[2]])
    con2[1].send([2,con1[2]])
    con1[1].recv()
    con2[1].recv()


def pause(svs):
    for s in svs:
        svs[s][1].send([3])
    for s in svs:
        svs[s][1].recv()


def start(svs):
    for s in svs:
        svs[s][1].send([4])
    for s in svs:
        svs[s][1].recv()


def stabilize(svs):
    #print('RUNNING STABILIZE')
    for s in svs:
        svs[s][1].send([5,len(svs)])
    for s in svs:
        #print('WAITING ON',s)
        svs[s][1].recv()
        #print('GOT RESPONSE FROM',s)


def printLog(svs,sid):
    svs[sid][1].send([6])
    svs[sid][1].recv()


def put(cid,cls,songname,url):
    #print(cid,'PUT')
    cls[cid][1].send([7,songname,url])
    cls[cid][1].recv()


def get(cid,cls,songname):
    #print(cid,'GET')
    cls[cid][1].send([8,songname])
    cls[cid][1].recv()


def delete(cid,cls,songname):
    #print(cid,'DELETE')
    cls[cid][1].send([9,songname])
    cls[cid][1].recv()


if __name__ == "__main__":
    pid = open('pidlog', 'w')
    pid.write(str(os.getpid())+'\n')
    pid.close()
    servers, clients, = {}, {}
    for line in fileinput.input():
        line = line.split()
        if line[0] == 'joinServer':
            sid = line[1]
            joinServer(servers,sid,clients)
        elif line[0] == 'retireServer':
            sid = line[1]
            retireServer(sid,servers)
        elif line[0] == 'joinClient':
            cid = line[1]
            sid = line[2]
            joinClient(cid,sid,servers,clients)
        elif line[0] == 'breakConnection':
            id1 = line[1]
            id2 = line[2]
            breakConnection(id1,id2,servers,clients)
        elif line[0] == 'restoreConnection':
            id1 = line[1]
            id2 = line[2]
            restoreConnection(id1,id2,servers,clients)
        elif line[0] == 'pause':
            pause(servers)
        elif line[0] == 'start':
            start(servers)
        elif line[0] == 'stabilize':
            stabilize(servers)
        elif line[0] == 'printLog':
            sid = line[1]
            printLog(servers,sid)
        elif line[0] == 'put':
            cid = line[1]
            songname = line[2]
            url = line[3]
            put(cid,clients,songname,url)
        elif line[0] == 'get':
            cid = line[1]
            songname = line[2]
            get(cid,clients,songname)
        elif line[0] == 'delete':
            cid = line[1]
            songname = line[2]
            delete(cid,clients,songname)
        else:
            print('UNKNOWN COMMAND')

    #print('END OF FILE')
    for s in servers:
        servers[s][0].terminate()
    for c in clients:
        clients[c][0].terminate()