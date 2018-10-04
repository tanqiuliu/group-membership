import socket
import pingAck_pb2

UDP_IP = "fa18-cs425-g45-01.cs.illinois.edu"
UDP_PORT = 5005
MESSAGE = "HELLO"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

message = pingAck_pb2.pingAck()
message.sourceId = 32
message.seqNum = 99
message.isAck = False
message.type = pingAck_pb2.pingAck.JOIN
message.members.extend([1, 2, 3, 4, 5, 6])

print(message.sourceId)
print(message.seqNum)
print(message.isAck)
print(message.type)
print(message.members)

print(message)

byteMessage = message.SerializeToString()

print(byteMessage)

sock.sendto(byteMessage, (UDP_IP, UDP_PORT))
