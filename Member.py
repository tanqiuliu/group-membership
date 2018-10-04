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
MAXDATASIZE = 1024
LOGPATH = 'log'
logging.basicConfig(level=logging.DEBUG)

class MemberInfo(object):
    def __init__(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port
        
class Member(object):
    

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        # self.id = ip + ':' + str(port) + '_' + datetime.datetime.now().isoformat()
        self.id = ip + ':' + str(port) + '_' + "2018-10-02T15:08:03.614879"     # for debug
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((ip, port))
        self.ackQueue = []
        self.ackQueueLock = threading.Lock()
        self.eventQueue = []
        self.eventQueueLock = threading.Lock()
        # self.memberList = {}    # key = MemberInfo.id, value = MemberInfo
        self.memberList = {
            "127.0.0.1:12301_2018-10-02T15:08:03.614879": MemberInfo("127.0.0.1:12301_2018-10-02T15:08:03.614879", "127.0.0.1", 12301),
            "127.0.0.1:12302_2018-10-02T15:08:03.614879": MemberInfo("127.0.0.1:12302_2018-10-02T15:08:03.614879", "127.0.0.1", 12302),
            "127.0.0.1:12303_2018-10-02T15:08:03.614879": MemberInfo("127.0.0.1:12303_2018-10-02T15:08:03.614879", "127.0.0.1", 12303),
            "127.0.0.1:12304_2018-10-02T15:08:03.614879": MemberInfo("127.0.0.1:12304_2018-10-02T15:08:03.614879", "127.0.0.1", 12304),
            "127.0.0.1:12305_2018-10-02T15:08:03.614879": MemberInfo("127.0.0.1:12305_2018-10-02T15:08:03.614879", "127.0.0.1", 12305),
        }
        self.period = 1   # in seconds
        self.seqNum = 0
        self.runRecv()
        self.runPing()

    def ping(self, target_id):
        if target_id not in self.memberList:
            logging.debug("%s is not in the memberList" %target_id)
            return
        target_ip = self.memberList[target_id].ip
        target_port = self.memberList[target_id].port
        logging.debug("ping to {}, seqNum = {}, t = {:.4f}".format(target_id, self.seqNum, time.time()))
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
        while(True):
            time.sleep(next(g))
            if c == len(curMemberIdList):
                curMemberIdList = list(self.memberList.keys())
                random.shuffle(curMemberIdList)
                c = 0
                print("==========================================================================================")
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
                        logging.warning("%s failed!" %prev_target_id)
                self.ackQueue = []
            if len(curMemberIdList)  == 0:  
                continue
            self.ping(curMemberIdList[c])
            prev_target_id = curMemberIdList[c]
            # update memberList, make sure update after ping since updating memberList will empty eventQueue
            self.updateMemberList()
            c += 1
            self.seqNum += 1

    def updateMemberList(self):
        with self.eventQueueLock:
            for event in self.eventQueue:
                if event.eventType == membership_pb2.Event.JOIN:
                    member = MemberInfo(event.memberId, event.memberIp, event.memberPort)
                    self.memberList[member.id] = member
                elif event.eventType == membership_pb2.Event.LEAVE:
                    if event.memberId in self.memberList:
                        self.memberList.pop(event.memberId)
                elif event.eventType == membership_pb2.Event.FAIL:
                    if event.memberId in self.memberList:
                        self.memberList.pop(event.memberId)
                        logging.debug("%s is removed from memberList" %event.memberId)
            self.eventQueue = []
                
    def _runRecv(self):
        while(True):
            msgRecvd = membership_pb2.PingAck()
            data, their_addr = self.sock.recvfrom(MAXDATASIZE)
            msgRecvd.ParseFromString(data)
            logging.info("received %s from %s" %(msgRecvd.msgType, msgRecvd.sourceId))
            if msgRecvd.msgType == membership_pb2.PingAck.PING:
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
        th = threading.Thread(target=self._runRecv).start()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python Member.py <PORT>")
        sys.exit()

    port = int(sys.argv[1])
    # ip = socket.gethostbyname(socket.gethostname())
    ip = '127.0.0.1'
    if(not os.path.isdir(LOGPATH)):
        os.makedirs(LOGPATH)
    member = Member(ip, port)
