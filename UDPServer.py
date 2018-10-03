import socket

UDP_IP = "127.0.0.1"
UDP_PORT = 12345

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock.bind(('', UDP_PORT))

while True:
    data, addr = sock.recvfrom(1024)
    print ("received message: " + data.decode())