import socket
import sys
import time
import datetime
import threading 
# from utils import *
import random

# PORT = 10123
MAXDATASIZE = 1024

class Member(object):
    def __init__(self, ip, port, isIntroducer=False):
        self.addr = ip, port
        self.isIntroducer = isIntroducer
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ackQueueLock = threading.Lock()
        self.sock.bind((ip, port))
        # self.id = ip + ':' + str(port) + '_' + datetime.datetime.now().isoformat()
        self.id = ip + ':' + str(port) + '_' + "2018-10-02T15:08:03.614879"
        self.memberList = [
            "127.0.0.1:12301_2018-10-02T15:08:03.614879",
            "127.0.0.1:12302_2018-10-02T15:08:03.614879",
            "127.0.0.1:12303_2018-10-02T15:08:03.614879",
            "127.0.0.1:12304_2018-10-02T15:08:03.614879",
            "127.0.0.1:12305_2018-10-02T15:08:03.614879"
        ]
        self.period = 1   # in seconds
        self.ackQueue = []
        self.eventQueue = []
        self.seqNum = 0
        self.runRecv()
        self.runPing()

    def ping(self, target_id):
        target_ip = target_id.split('_')[0].split(':')[0]
        target_port = int(target_id.split('_')[0].split(':')[1])
        print("ping to {}, seqNum = {}, t = {:.4f}".format(target_id, self.seqNum, time.time()))
        msg = "ping,{},{}".format(self.id, str(self.seqNum))
        self.sock.sendto(msg.encode(), (target_ip, target_port))

    def runPing(self):
        def g_tick():
            t = time.time()
            count = 0
            while True:
                count += 1
                yield max(t + count * self.period - time.time(), 0)

        curMemberList = self.memberList.copy()
        c = 0
        prev_target_id = ""
        g = g_tick()
        while(True):
            time.sleep(next(g))
            if c == len(curMemberList):
                # update memberList according to eventQueue
                curMemberList = self.memberList.copy()
                random.shuffle(curMemberList)
                c = 0
            # check if recv ack
            with self.ackQueueLock:
                if (prev_target_id, self.seqNum - 1) not in self.ackQueue and prev_target_id != "":
                    self.eventQueue.append((prev_target_id, "FAIL"))
                    print("%s failed!" %prev_target_id)
                self.ackQueue = []
            self.ping(curMemberList[c])
            prev_target_id = curMemberList[c]
            c += 1
            self.seqNum += 1
                
    def _runRecv(self):
        while(True):
            msg, their_addr = self.sock.recvfrom(MAXDATASIZE)
            msgtype, source_id, msgSeqNum = msg.decode().split(',')
            print("received %s from %s" %(msgtype, source_id))
            if msgtype == 'ping':
                ack_msg = "ack,{},{}".format(self.id, str(msgSeqNum))
                self.sock.sendto(ack_msg.encode(), their_addr)
            elif msgtype == 'ack':
                with self.ackQueueLock:
                    self.ackQueue.append((source_id, int(msgSeqNum)))
    
    def runRecv(self):
        th = threading.Thread(target=self._runRecv).start()
            


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python Member.py <PORT>")
    else:
        port = int(sys.argv[1])
        # ip = socket.gethostbyname(socket.gethostname())
        ip = '127.0.0.1'
        member = Member(ip, port)
