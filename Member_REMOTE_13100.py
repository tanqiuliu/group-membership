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
        self.pingTimeout = 0.2
        self.pingReqK = 3
        self.seqNum = 1
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
        if random.random() < 0.8:       # randomly drop some packet to test ping-req
            self.sock.sendto(msg.SerializeToString(), (target_ip, target_port))

    def pingReq(self, target_id):
        if target_id not in self.memberList:
            logging.debug("%s is not in the memberList" %target_id)
            return
        curMemberIdList = list(self.memberList.keys())
        random.shuffle(curMemberIdList)
        indirectMembers = curMemberIdList[0:self.pingReqK]
        msg = self.constructPingReqMsg(target_id)
        for memberId in indirectMembers:
            candi_addr = self.memberList[memberId].ip, self.memberList[memberId].port
            self.sock.sendto(msg.SerializeToString(), candi_addr)


    def runPing(self):
        def g_tick():
            t = time.time()
            count = 0
            while True:
                count += 1
                yield max(t + (count - 1) * self.period + self.pingTimeout - time.time(), 0)
                yield max(t + count * self.period - time.time(), 0)

        curMemberIdList = list(self.memberList.keys())
        c = 0
        prev_target_id = ""
        pingReqFlag = False
        g = g_tick()
        while(True):
            if c == len(curMemberIdList):
                curMemberIdList = list(self.memberList.keys())
                random.shuffle(curMemberIdList)
                c = 0
                print("==========================================================================================")
            if len(curMemberIdList)  == 0:  
                logging.warning("No member alive!")
                break
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
            self.ping(curMemberIdList[c])
            prev_target_id = curMemberIdList[c]
            # update memberList, make sure update after ping since updating memberList will empty eventQueue
            self.updateMemberList()
            time.sleep(next(g))
            # check if recv this ping's ack, else ping-req
            with self.ackQueueLock:
                if (prev_target_id, self.seqNum) not in self.ackQueue and prev_target_id != "":
                    pingReqFlag = True
            if pingReqFlag:
                logging.debug("Did not receive ack from %s, sending ping-req." %prev_target_id)
                self.pingReq(curMemberIdList[c])
                pingReqFlag = False
            c += 1
            self.seqNum += 1
            time.sleep(next(g))

    def updateMemberList(self):
        with self.eventQueueLock:
            print(self.eventQueue)
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

    def runRecv(self):
        th = threading.Thread(target=self._runRecv).start()

    def _runRecv(self):
        while(True):
            msgRecvd = membership_pb2.PingAck()
            data, their_addr = self.sock.recvfrom(MAXDATASIZE)
            try:
                msgRecvd.ParseFromString(data)
            except:
                print(data)
                print(msgRecvd)
            logging.info("received %s from %s" %(msgRecvd.msgType, msgRecvd.sourceId))
            # append the piggybacked events to local eventQueue
            with self.eventQueueLock:
                for event in msgRecvd.events:
                    if event not in self.eventQueue:        # avoid duplicate events, need a expiration mechanism according to period
                        self.eventQueue.append(event)
            # handle different types of messages
            if msgRecvd.msgType == membership_pb2.PingAck.PING:
                if msgRecvd.seqNum > 0:
                    ack_msg = self.constructAckMsg(msgRecvd)
                    self.sock.sendto(ack_msg.SerializeToString(), their_addr)
                elif msgRecvd.seqNum < 0:
                    assert msgRecvd.targetId != None
                    ack_msg = self.constructAckReqMsg(msgRecvd)
                    self.sock.sendto(ack_msg.SerializeToString(), their_addr)
            elif msgRecvd.msgType == membership_pb2.PingAck.ACK:
                with self.ackQueueLock:
                    self.ackQueue.append((msgRecvd.sourceId, abs(msgRecvd.seqNum)))
            elif msgRecvd.msgType == membership_pb2.PingAck.PINGREQ:
                assert msgRecvd.targetId != None and msgRecvd.seqNum < 0
                if msgRecvd.targetId not in self.memberList:
                    continue
                indirect_ping = self.constructIndirectPingMsg(msgRecvd)
                ping_target_addr = self.memberList[msgRecvd.targetId].ip, self.memberList[msgRecvd.targetId].port
                self.sock.sendto(indirect_ping.SerializeToString(), ping_target_addr)
            elif msgRecvd.msgType == membership_pb2.PingAck.ACKREQ:
                assert msgRecvd.targetId != None and msgRecvd.seqNum < 0
                if msgRecvd.targetId not in self.memberList:
                    continue
                indirect_ack = self.constructIndirectAckMsg(msgRecvd)
                ack_target_addr = self.memberList[msgRecvd.targetId].ip, self.memberList[msgRecvd.targetId].port
                self.sock.sendto(indirect_ack.SerializeToString(), ack_target_addr)
            

    def constructPingMsg(self):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.seqNum = self.seqNum
        msg.msgType = membership_pb2.PingAck.PING
        self.piggybackEvents(msg)
        return msg
    
    def constructAckMsg(self, ping_msg):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.seqNum = ping_msg.seqNum
        msg.msgType = membership_pb2.PingAck.ACK
        self.piggybackEvents(msg)
        return msg

    def constructPingReqMsg(self, targetId):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.targetId = targetId
        msg.seqNum = -1 * self.seqNum
        msg.msgType = membership_pb2.PingAck.PINGREQ
        self.piggybackEvents(msg)
        return msg
    
    def constructIndirectPingMsg(self, pingreq_msg):
        msg = membership_pb2.PingAck()
        msg.targetId = pingreq_msg.targetId
        msg.sourceId = pingreq_msg.sourceId
        msg.seqNum = pingreq_msg.seqNum
        msg.msgType = membership_pb2.PingAck.PING
        self.piggybackEvents(msg)
        return msg

    def constructAckReqMsg(self, indirect_ping):
        msg = membership_pb2.PingAck()
        msg.targetId = indirect_ping.sourceId
        msg.sourceId = self.id
        msg.seqNum = indirect_ping.seqNum
        msg.msgType = membership_pb2.PingAck.ACKREQ
        self.piggybackEvents(msg)
        return msg

    def constructIndirectAckMsg(self, ackreq_msg):
        msg = membership_pb2.PingAck()
        msg.targetId = ackreq_msg.targetId
        msg.sourceId = ackreq_msg.sourceId
        msg.seqNum = ackreq_msg.seqNum
        msg.msgType = membership_pb2.PingAck.ACK
        self.piggybackEvents(msg)
        return msg
    
    def piggybackEvents(self, msg):
        with self.eventQueueLock:
            for event in self.eventQueue:
                event_piggybacked = msg.events.add()
                event_piggybacked.eventType = event.eventType
                event_piggybacked.memberId = event.memberId
                event_piggybacked.memberIp = event.memberIp
                event_piggybacked.memberPort = event.memberPort


    
    


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
