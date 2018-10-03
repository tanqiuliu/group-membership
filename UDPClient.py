import socket

UDP_IP = "fa18-cs425-g45-01.cs.illinois.edu"
UDP_PORT = 5005
MESSAGE = "HELLO"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock.sendto(MESSAGE.encode(), (UDP_IP, UDP_PORT))
