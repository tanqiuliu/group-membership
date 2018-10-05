import socket
import sys
import os
import time
import datetime
import threading 
import logging
import random
import membership_pb2


# PORT = 10123
cmd = ""
MAXDATASIZE = 1024
LOGPATH = 'log'
logging.basicConfig(level=logging.DEBUG)

class MemberInfo(object):
    def __init__(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port
        
class Member(object):
    

    def __init__(self, ip, port, introducerId=""):
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((ip, port))
        self.ackQueue = []
        self.ackQueueLock = threading.Lock()
        self.eventQueue = []
        self.eventQueueLock = threading.Lock()
        # self.memberList = {}    # key = MemberInfo.id, value = MemberInfo
        self.memberList = {
        }
        self.period = 1   # in seconds
        self.seqNum = 0
        if introducerId == "":
            # self.id = ip + ':' + str(port) + '_' + datetime.datetime.now().isoformat()
            self.id = ip + ':' + str(port) + '_' + "2018-10-02T15:08:03.614879"     # for debug
        else:
            self.id = "Introducer"
        self.runRecv()
        self.runPingThreaded()

    # If pingNum = 0, its a normal ping, 1 is a joinPing for first sending to introducer, 2 is a leavePing to all other nodes
    def ping(self, target_id, pingNum=0):
        if target_id not in self.memberList:
            logging.debug("%s is not in the memberList" %target_id)
            return
        target_ip = self.memberList[target_id].ip
        target_port = self.memberList[target_id].port
        #logging.debug("ping to {}, seqNum = {}, t = {:.4f}".format(target_id, self.seqNum, time.time()))

        msg = None
        if pingNum == 2:
            msg = self.constructLeavingPingMsg()
        elif pingNum == 1:
            msg = self.constructJoiningPingMsg()
        else:
            msg = self.constructPingMsg()
        self.sock.sendto(msg.SerializeToString(), (target_ip, target_port))

    def constructPingMsg(self):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.seqNum = self.seqNum
        msg.msgType = membership_pb2.PingAck.PING
        with self.eventQueueLock:
            for event in self.eventQueue:
                event_piggybacked = msg.events.add()
                event_piggybacked.eventType = event.eventType
                event_piggybacked.memberId = event.memberId
                event_piggybacked.memberIp = event.memberIp
                event_piggybacked.memberPort = event.memberPort
        return msg

    def constructLeavingPingMsg(self):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.seqNum = self.seqNum
        msg.msgType = membership_pb2.PingAck.PING
        event = msg.events.add()
        event.eventType = membership_pb2.Event.LEAVE
        event.memberId = self.id
        event.memberIp = self.ip
        event.memberPort = self.port
        return msg

    def constructJoiningPingMsg(self):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.seqNum = self.seqNum
        msg.msgType = membership_pb2.PingAck.PING
        event = msg.events.add()
        event.eventType = membership_pb2.Event.JOIN
        event.memberId = self.id
        event.memberIp = self.ip
        event.memberPort = self.port
        return msg

    def runPing(self):
        def g_tick():
            t = time.time()
            count = 0
            while True:
                count += 1
                yield max(t + count * self.period - time.time(), 0)

        curMemberIdList = list(self.memberList.keys())
        c = 0
        prev_target_id = ""
        g = g_tick()
        while cmd != "Leave":
            time.sleep(next(g))
            if c >= len(curMemberIdList):
                curMemberIdList = list(self.memberList.keys())
                random.shuffle(curMemberIdList)
                c = 0
            # check if recv ack
            with self.ackQueueLock:
                if (prev_target_id, self.seqNum - 1) not in self.ackQueue and prev_target_id != "":
                    if prev_target_id in self.memberList:
                        failEvent = membership_pb2.Event()
                        failEvent.eventType = membership_pb2.Event.FAIL
                        failEvent.memberId = prev_target_id
                        failEvent.memberIp = self.memberList[prev_target_id].ip
                        failEvent.memberPort = self.memberList[prev_target_id].port
                        self.eventQueue.append(failEvent)
                        #logging.warning("%s failed!" %prev_target_id)
                self.ackQueue = []


            if (len(curMemberIdList) - 1) != -1:
                self.ping(curMemberIdList[c])
                prev_target_id = curMemberIdList[c]
            # update memberList, make sure update after ping since updating memberList will empty eventQueue
            self.updateMemberList()

            if (len(list(self.memberList.keys()))) != -1:
                c += 1
            self.seqNum += 1
        print("The thread is still running")

    def updateMemberList(self):
        with self.eventQueueLock:
            for event in self.eventQueue:
                if event.eventType == membership_pb2.Event.JOIN:
                    member = MemberInfo(event.memberId, event.memberIp, event.memberPort)
                    if member.id != self.id and not member.id in self.memberList.keys():
                        print("We have a new member joining who's ID is: " + str(member.id) + " Ip:" + str(member.ip) + " Port:" +  str(member.port))
                        self.memberList[member.id] = member
                    else:
                        continue
                elif event.eventType == membership_pb2.Event.LEAVE:
                    print("We received a node Leave event from ip: " + event.memberIp)
                    if event.memberId in self.memberList:
                        self.memberList.pop(event.memberId)
                elif event.eventType == membership_pb2.Event.FAIL:
                    if event.memberId in self.memberList:
                        print("We received a failure from node : " + str(event.memberId))
                        self.memberList.pop(event.memberId)
                        #logging.debug("%s is removed from memberList" %event.memberId)
            self.eventQueue = []
                
    def _runRecv(self):
        while True:
            msgRecvd = membership_pb2.PingAck()
            data, their_addr = self.sock.recvfrom(MAXDATASIZE)
            msgRecvd.ParseFromString(data)
            #logging.info("received %s from %s" %(msgRecvd.msgType, msgRecvd.sourceId))
            if msgRecvd.msgType == membership_pb2.PingAck.PING:
                if not msgRecvd.sourceId in self.memberList.keys():
                    print("We have a new member joining who's ID is: " + str(msgRecvd.sourceId) + " Ip:" + str(their_addr[0]) + " Port:" + str(their_addr[1]))
                    newmember = MemberInfo(msgRecvd.sourceId, their_addr[0], their_addr[1])
                    self.memberList[msgRecvd.sourceId] = newmember
                ack_msg = self.constructAckMsg(msgRecvd)
                self.sock.sendto(ack_msg.SerializeToString(), their_addr)
            elif msgRecvd.msgType == membership_pb2.PingAck.ACK:
                with self.ackQueueLock:
                    self.ackQueue.append((msgRecvd.sourceId, msgRecvd.seqNum))
            with self.eventQueueLock:
                for event in msgRecvd.events:
                    self.eventQueue.append(event)



    def constructAckMsg(self, ping_msg):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.seqNum = ping_msg.seqNum
        msg.msgType = membership_pb2.PingAck.ACK
        return msg
    
    def runRecv(self):
        th = threading.Thread(target=self._runRecv)
        th.daemon = True
        th.start()

    def runPingThreaded(self):
        rp = threading.Thread(target=self.runPing)
        rp.daemon = True
        rp.start()


if __name__ == "__main__":
    #Error message if the person didn't add a port
    if len(sys.argv) < 2:
        print("Usage: python Member.py <PORT> <'isIntroducer'(sets this node to be introducer)>")
        sys.exit()

    port = int(sys.argv[1])
    # ip = socket.gethostbyname(socket.gethostname())
    ip = socket.gethostname()
    if(not os.path.isdir(LOGPATH)):
        os.makedirs(LOGPATH)

    print("Starting up server at ip: " + ip)

    #Sets up the member to be an introducer or not based on the 2nd parameter
    member = None
    if(len(sys.argv) == 3):
        member = Member(ip, port, 'isIntroducer')
    else:
        member = Member(ip, port)

    #While the cmd is not to Leave the membership list, we want to offer giving out the Id, current membership list, or
    #join through the introducer (The introducer will never use the Join function)
    while cmd != "Leave":
        cmd = input("Options are Leave, Members, Id, and Join: ")
        if cmd == "Id":
            print(member.id)
        elif cmd == "Members":
            print("The ids in the membership list are: \n ===================== \n" + "\n".join(str(x) for x in member.memberList.keys()) + "\n ===================== \n")
        elif cmd == "Join":
            member.memberList['Introducer'] = MemberInfo('Introducer', 'fa18-cs425-g45-01.cs.illinois.edu', 12345)
            member.ping('Introducer', 1)

    print("Node " + member.id + " has now left the membership list at: " + str(datetime.datetime.now()))
    sys.exit(-1)

