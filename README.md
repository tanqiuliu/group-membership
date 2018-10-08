# SWIM-like group membership

CS425 FA18 MP2

Usage:
#### Start the Introducer:
sudo python3.6 member.py <port> isIntroducer
#### Start other nodes:
sudo python3.6 member.py <port>
In the interactive mode: enter "Join" to join the node; enter "Leave" to leave, enter "Members" to print the member list of this node. 
Note that the introducer must use port 12345.
#### example:
sudo python3.6 member.py 12345 isIntroducer
sudo python3.6 member.py 12345
