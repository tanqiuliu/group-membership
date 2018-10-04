from threading import Thread, enumerate as t_enum
import socket

UDP_IP = "fa18-cs425-g45-01.cs.illinois.edu"
UDP_PORT = 5005

id = 1
member = [1, 2, 3, 4, 5]
comm = ""

def threadedRecvFrom():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', UDP_PORT))

    while comm != "Leave":
        data, addr = sock.recvfrom(1024)
        print(data)

if __name__ == '__main__':

    Thread(target=threadedRecvFrom).start()

    while comm != "Leave":
        comm = input("State your thing : ")
        if comm == "ID":
            print(id)
        elif comm == "Members":
            print(" ".join(str(i) for i in member))

