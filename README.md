# DistributedSystems-Project-2021
A NTUA project for the Distributed Systems course 

This project is a design of a ToyChord, a simpler version of Chord, as described in [Stoica, Ion, et al. "Chord: A scalable peer-to-peer lookup service for internet applications." ACMSIGCOMM Computer Communication Review 31.4 (2001): 149-160).](https://dl.acm.org/doi/10.1145/964723.383071). 

It is a file sharing application that utilizes many distributed nodes DHT. Every node performs every DHT operation. This involves interactions with Chord either as a server or as a client, opening sockets and answering requests.The nodes keep information in key-value pairs. This application is impimented with two types of consistency, linearizability and eventual.

# In order for the code to run:

 - Run python3 ToyChordServer.py
  (after that both ip and port will show up)

 - Run python3 ToyNode_Linearizability.py and give the ip and the port that server replied before. 
	or 
 - Run python3 ToyNode_Eventual_Consistency.py and give the ip and the port that server replied before. 

 - Run python3 ToyChordServer_Cli.py  and give the ip and the port that server replied before.
   (so as to make requests from server node)
