import socket
import pingAck_pb2

UDP_IP = "fa18-cs425-g45-01.cs.illinois.edu"
UDP_PORT = 5005

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock.bind(('', UDP_PORT))

anotherMessage = pingAck_pb2.pingAck()

while True:
	data, addr = sock.recvfrom(1024)
	anotherMessage.ParseFromString(data)
	print(anotherMessage)
	print(anotherMessage.members)
	print(anotherMessage.members[0])
	print("Received : " + anotherMessage)
