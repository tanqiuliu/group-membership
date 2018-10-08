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
# logging.basicConfig(level=logging.DEBUG)

#We made this class to hold information of the id, port, and ip inside a membership list of one virtual machine for other nodes in the virtual machine
class MemberInfo(object):
    def __init__(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port
        
class Member(object):

    #The intitialization of our Member class has the basics such as ip, port, and the socket
    #The period determines how many pings and acks are being sent per period and with pingTimeOut helps determine the amount of time an ack must be sent back
    #The eventQueue holds propagated information about any JOIN, LEAVE, or FAIL events and updates the membership list appropiately
    #ackQueue helps us in detecting if any of the pings we sent to other nodes have failed or not
    #The id is composed of our ip, our port, and a timestamp
    #The seqNum is the amount of periods this particular vm has went through
    def __init__(self, ip, port, introducerId=""):
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((ip, port))
        self.ackQueue = []
        self.ackQueueLock = threading.Lock()
        self.eventQueue = []
        self.eventQueueLock = threading.Lock()
        self.memberList = {
        }
        self.period = 0.5   # in seconds
        if introducerId == "":
            self.id = ip + ':' + str(port) + '_' + datetime.datetime.now().isoformat()    # for debug
        else:
            self.id = "Introducer"
        self.leaving = -1
        self.pingTimeout = 0.15
        self.pingReqK = 3
        self.seqNum = 1
        self.initLogger('membership.log')
        self.runRecv()
        self.runPingThreaded()

    #Function to help initialize creation for a log composed of JOIN,Leave, and FAIL events along ith the pings and acks it receives inside membership.log
    def initLogger(self, logFilename):
        self.logger = logging.getLogger(self.id)
        self.logger.setLevel(logging.DEBUG)
        if os.path.exists(logFilename):
            os.remove(logFilename)
        self.fh = logging.FileHandler(logFilename)
        self.fh.setLevel(logging.DEBUG)
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fh.setFormatter(formatter)
        self.ch.setFormatter(formatter)
        self.logger.addHandler(self.fh)
        self.logger.addHandler(self.ch)

    #A function that helped us manipulate the percent drop rate for experimenting on the report
    def sendto(self, data, addr):
        if random.random() > 0:
            self.sock.sendto(data, addr)

    # If pingNum = 0, its a normal failure detection ping, 1 is a joinPing for first sending to introducer, 2 is a leavePing to all other nodes
    # 3 is a ping from the introducer to all other nodes for a joining node, and 4 is a ping from the introducer to the joining node which contains
    # the Introducer's current membership list.
    def ping(self, target_id, pingNum=0, newmember=None):
        if target_id not in self.memberList:
            self.logger.debug("%s is not in the memberList" %target_id)
            return
        target_ip = self.memberList[target_id].ip
        target_port = self.memberList[target_id].port
        self.logger.debug("ping to {}, seqNum = {}".format(target_id, self.seqNum))
        msg = None
        if pingNum == 3:
            msg = self.constructJoiningPingMsg(True, newmember)
        elif pingNum == 2:
            msg = self.constructLeavingPingMsg()
        elif pingNum == 1:
            msg = self.constructJoiningPingMsg()
        elif pingNum == 4:
            msg = self.constructIntroducerToReceiverMsg()
        else:
            msg = self.constructPingMsg()

        # self.logger.debug("ping to {}, seqNum = {}, t = {:.4f} at addr: {}, port: {}".format(target_id, self.seqNum, time.time(), target_ip, target_port)) for debugging
        serialized_msg = msg.SerializeToString()
        #self.logger.debug("Experiment 1: Pinging amount : |" + str(len(serialized_msg)) + "| bytes at |" + str(time.time())) code that helped test Bps
        self.sendto(serialized_msg, (target_ip, target_port))

    #A special ping where we want to indirectly ask another machine to ping the target machine that we want because we didn't
    #receive an ack from the target machine
    def pingReq(self, target_id):
        if target_id not in self.memberList:
            #logging.debug("%s is not in the memberList" %target_id)
            return
        curMemberIdList = list(self.memberList.keys())
        random.shuffle(curMemberIdList)
        indirectMembers = None
        if len(curMemberIdList) >= 3:
            indirectMembers = curMemberIdList[0:self.pingReqK]
        else:
            indirectMembers = curMemberIdList
        msg = self.constructPingReqMsg(target_id)
        for memberId in indirectMembers:
            candi_addr = self.memberList[memberId].ip, self.memberList[memberId].port
            self.logger.debug("send ping-req to {}, targeting {}".format(candi_addr, target_id))
            self.sendto(msg.SerializeToString(), candi_addr)

    #Runs a threaded version of sending out one ping per period to the nodes in its current membership list
    #and occasionally also an indirect ping. Follows the swim-like protocol to reduce the amount of pings
    #per period and shuffles the current member list after finishing pinging to the length of the membership list as in the algorithm
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
        while self.leaving != 0:
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
                        if not prev_target_id == self.id:
                            self.eventQueue.append(failEvent)
                        self.logger.info("Failure detected {}".format(prev_target_id))
                self.ackQueue = []


            if (len(curMemberIdList) - 1) != -1:
                self.ping(curMemberIdList[c])
                prev_target_id = curMemberIdList[c]
            # update memberList, make sure update after ping since updating memberList will empty eventQueue
            self.updateMemberList()
            time.sleep(next(g))

            with self.ackQueueLock:
                if (prev_target_id, self.seqNum) not in self.ackQueue and prev_target_id != "":
                    pingReqFlag = True
            if pingReqFlag:
                self.logger.debug("did not receive ack from {}".format(prev_target_id))
                if c < len(self.memberList.keys()):
                    self.pingReq(curMemberIdList[c])
                pingReqFlag = False

            if (len(list(self.memberList.keys())) - 1) != -1:
                c += 1
            self.seqNum += 1
            time.sleep(next(g))

    def updateMemberList(self):
        with self.eventQueueLock:
            #Handles the events in the eventQueue where if its a JOIN we want to add it into the membership list,
            #if its a LEAVE or FAIL we want to drop it from the membership list. In the introducers special case,
            #if a new node joins we want to send that information to every node in the membership list as well.
            for event in self.eventQueue:
                if event.eventType == membership_pb2.Event.JOIN and self.id == "Introducer":
                    newmember = MemberInfo(event.memberId, event.memberIp, event.memberPort)
                    if newmember.id != self.id and not newmember.id in self.memberList.keys():
                        self.memberList[newmember.id] = newmember
                        for memberId in self.memberList.keys():
                            self.ping(memberId, 3, newmember)
                        self.ping(newmember.id, 4)
                elif event.eventType == membership_pb2.Event.JOIN:
                    member = MemberInfo(event.memberId, event.memberIp, event.memberPort)
                    if member.id != self.id and not member.id in self.memberList.keys():
                        self.logger.info("new member event: {}, add to member list".format(member.id))
                        self.memberList[member.id] = member
                elif event.eventType == membership_pb2.Event.LEAVE:
                    if event.memberId in self.memberList and event.memberId != self.id:
                        self.logger.info("leaving event: {}, remove from member list".format(event.memberId))
                        self.memberList.pop(event.memberId)
                elif event.eventType == membership_pb2.Event.FAIL:
                    if event.memberId in self.memberList:
                        self.logger.info("failure event: {} has failed, remove from member list".format(event.memberId))
                        self.memberList.pop(event.memberId)
                        
            self.eventQueue = []

    #Runs a threaded version of receiving acks and responding to them
    def _runRecv(self):
        while True:
            msgRecvd = membership_pb2.PingAck()
            data, their_addr = self.sock.recvfrom(MAXDATASIZE)
            msgRecvd.ParseFromString(data)
            #self.logger.debug("Experiment 1: Ack Received: |" + str(len(data)) + "| bytes at |" + str(time.time()))
            #logging.info("received %s from %s" %(msgRecvd.msgType, msgRecvd.sourceId))
            try:
                msgRecvd.ParseFromString(data)
            except:
                # print(data)
                # print(msgRecvd)
                self.logger.debug("Error encountered while parsing message")
            self.logger.debug("received {} from {}".format(msgRecvd.msgType, msgRecvd.sourceId))
            # append the piggybacked events to local eventQueue
            #To avoid duplicates and events sticking in the eventQueue while being propagated around, we need conditionals
            #that will not have the event be added if its already been seen or already been removed depending on the event
            with self.eventQueueLock:
                for event in msgRecvd.events:
                    if event not in self.eventQueue:        # avoid duplicate events, need a expiration mechanism according to period
                        if event.eventType == membership_pb2.Event.JOIN and event.memberId in self.memberList.keys():
                            continue
                        elif event.eventType == membership_pb2.Event.LEAVE and not event.memberId in self.memberList.keys():
                            continue
                        elif event.eventType == membership_pb2.Event.FAIL and not event.memberId in self.memberList.keys():
                            continue
                        else:
                            self.eventQueue.append(event)
            # handle different types of messages, but it mostly just sends back an ack.  Depending on the seqNum, positive is for
            # regular pings and negative seqNums are for indirect pings.
            if msgRecvd.msgType == membership_pb2.PingAck.PING:
                if msgRecvd.seqNum > 0:
                    '''
                    if (self.id != "Introducer") and (not msgRecvd.sourceId in self.memberList.keys()) and (self.id != msgRecvd.sourceId):
                        #print("We have a new member joining who's ID is: " + str(msgRecvd.sourceId) + " Ip:" + str(their_addr[0]) + " Port:" + str(their_addr[1]))
                        #Marked: Node receives ping and adds msgRecvd sourceId into memberList
                        self.logger.info("received ping from unknown node: {}".format(msgRecvd.sourceId))
                        newmember = MemberInfo(msgRecvd.sourceId, their_addr[0], their_addr[1])
                        self.memberList[msgRecvd.sourceId] = newmember
                    '''
                    ack_msg = self.constructAckMsg(msgRecvd)
                    self.sendto(ack_msg.SerializeToString(), their_addr)
                elif msgRecvd.seqNum < 0:
                    assert msgRecvd.targetId != None
                    ack_msg = self.constructAckReqMsg(msgRecvd)
                    self.sendto(ack_msg.SerializeToString(), their_addr)
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
                self.sendto(indirect_ack.SerializeToString(), ack_target_addr)

    #Constructs a regular ping message that is the core of our failure detection where we poke at a node to see if it sends back an ack
    def constructPingMsg(self):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.seqNum = self.seqNum
        msg.msgType = membership_pb2.PingAck.PING
        self.piggybackEvents(msg)
        return msg

    #Constructs a ping with the leave event to tell every node in the membership list that it is leaving (NOT FAILING)
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

    #A special case where the introducer constructs a message with all the nodes in its membership list and sends it to the
    #joining node so that the joining node can catch up with all the nodes currently in the main membership list
    def constructIntroducerToReceiverMsg(self):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.seqNum = self.seqNum
        msg.msgType = membership_pb2.PingAck.PING
        for member in self.memberList.keys():
            event = msg.events.add()
            event.eventType = membership_pb2.Event.JOIN
            event.memberId = self.memberList[member].id
            event.memberIp = self.memberList[member].ip
            event.memberPort = self.memberList[member].port
        return msg

    #In the special case where we need the introducer to send to all the nodes in its membership list, the joining nodes information,
    #we use the introduced version, otherwise we use the regular version which is just the joining node sending information to the Introducer
    #that it wants to join in on the membership list
    def constructJoiningPingMsg(self, introduced=False, introducedinfo=None):
        if introduced:
            msg = membership_pb2.PingAck()
            msg.sourceId = introducedinfo.id
            msg.seqNum = self.seqNum
            msg.msgType = membership_pb2.PingAck.PING
            event = msg.events.add()
            event.eventType = membership_pb2.Event.JOIN
            event.memberId = introducedinfo.id
            event.memberIp = introducedinfo.ip
            event.memberPort = introducedinfo.port
            return msg
        else:
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

    #Constructs a regular ack msg that will be sent back to the machine that pinged it to let it know it's alive
    def constructAckMsg(self, ping_msg):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.seqNum = ping_msg.seqNum
        msg.msgType = membership_pb2.PingAck.ACK
        self.piggybackEvents(msg)
        return msg

    #Msg that requests another machine to ping to the actual machine we want to ping to but didnt reach (targetId)
    def constructPingReqMsg(self, targetId):
        msg = membership_pb2.PingAck()
        msg.sourceId = self.id
        msg.targetId = targetId
        msg.seqNum = -1 * self.seqNum
        msg.msgType = membership_pb2.PingAck.PINGREQ
        self.piggybackEvents(msg)
        return msg

    #Function to make a msg to ping to another machine so that we can indirectly ask our target machine if it is still alive
    def constructIndirectPingMsg(self, pingreq_msg):
        msg = membership_pb2.PingAck()
        msg.targetId = pingreq_msg.targetId
        msg.sourceId = pingreq_msg.sourceId
        msg.seqNum = pingreq_msg.seqNum
        msg.msgType = membership_pb2.PingAck.PING
        self.piggybackEvents(msg)
        return msg

    #Request the machine we pinged to send an ack of the target machine we wanted to receive an ack from
    def constructAckReqMsg(self, indirect_ping):
        msg = membership_pb2.PingAck()
        msg.targetId = indirect_ping.sourceId
        msg.sourceId = self.id
        msg.seqNum = indirect_ping.seqNum
        msg.msgType = membership_pb2.PingAck.ACKREQ
        self.piggybackEvents(msg)
        return msg

    #We want to ping another vm to ask them to ping to our target machine to send a chain of pings in case a message drops
    def constructIndirectAckMsg(self, ackreq_msg):
        msg = membership_pb2.PingAck()
        msg.targetId = ackreq_msg.targetId
        msg.sourceId = ackreq_msg.sourceId
        msg.seqNum = ackreq_msg.seqNum
        msg.msgType = membership_pb2.PingAck.ACK
        self.piggybackEvents(msg)
        return msg

   #This function helps propagate any JOIN, FAIL, or LEAVE events for pings
    def piggybackEvents(self, msg):
        with self.eventQueueLock:
            for event in self.eventQueue:
                if event.memberId != self.id:
                    event_piggybacked = msg.events.add()
                    event_piggybacked.eventType = event.eventType
                    event_piggybacked.memberId = event.memberId
                    event_piggybacked.memberIp = event.memberIp
                    event_piggybacked.memberPort = event.memberPort

    #Same reason as stated below
    def runRecv(self):
        th = threading.Thread(target=self._runRecv)
        th.daemon = True
        th.start()

    #We run the function in a threaded version without this thread being the main thread so we can input commands
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
    #joins through the introducer (The introducer will never use the Join function and is always vm1 and with port 12345)
    while True:
        cmd = input("Options are Leave, Members, Id, and Join: ")
        if cmd == "Id":
            print(member.id)
        elif cmd == "Members":
            print("The ids in the membership list are: \n ===================== \n" + "\n".join(str(x) for x in member.memberList.keys()) + "\n ===================== \n")
        elif cmd == "Join":
            member.memberList['Introducer'] = MemberInfo('Introducer', 'fa18-cs425-g45-01.cs.illinois.edu', 12345)
            member.ping('Introducer', 1)
        elif cmd == "Leave":
            member.leaving = len(member.memberList.keys())
            break;

    #Once a node has initialized the leaving state, we want to disseminate a leaving ping to all other nodes
    member.leaving = 0
    for memberId in member.memberList.keys():
        member.ping(memberId, 2)

    print("Node " + member.id + " has now left the membership list at: " + str(datetime.datetime.now()))
    sys.exit(-1)

