#                                                                                                     #
#               ToyChord Node's code with Eventual Consistency implementation in query request        #
#######################################################################################################

import socket 
from threading import Thread 
import sys
import pickle
import threading
import signal
import hashlib
import errno
import fcntl,os
#from time import sleep,time
from datetime import datetime
import select
import time

#-------------------------------------------- Global Variables -------------------------------------------------------#
s1 = None
m = 10 #number of bits per key
size = 2 ** m #number of keys of m bits
QUERY_DONE = False
SEARCH_DONE = None
DELETE_DONE = False
IsThereConn = 0
Socket2 = None
Instr_Socket = None
Replay_socket = None
CLI_THREAD = None
NodeInfo = {"id":0,"ip_pred":"","pred":0,"succ":None}
server_NODE_socket = None
k_factor = 2
Name_of_file = "query.txt"
filename = "event_with_f_" + str(k_factor) + "_"+Name_of_file.split(".")[0]+ ".txt"
DataList = [] 
Replicas = []
listeningPort = 0
listeningPort_ip = ""
LISTENING_REPLAY_PORT = 0
LISTENING_REPLAY_IP = ""
THREADS = []
t0 = 0
host_S = ""
port_S = 0

################################################### - FUNCTIONS - ########################################################

#-------------------------------------------- HOW_MANY_BYTES -----------------------------------------------------------#
'''
This function returns the number of bytes of a string
'''

def HOW_MANY_BYTES(s):
    return len(s.encode('utf-8'))

#------------------------------------------------- HELP_INFO -----------------------------------------------------------#
'''
This function that returns a string with all the options which are available for the client
'''

def HELP_INFO():
  
    strr ="""\t____ HELP INFO ____\n1. INSERT  <port> <value>\nIn this option a node join the network with port: <port>\n 
and link for the node in which information is stored in DHT: <value>.\n
2. DELETE <port>\nIn this option a node, with port number: <port>, is deleted from DHT.\n 
3. QUERY <port>\nIn this option, port of the node is given as parameter in order to find the 
key of this node and the data of this one are returned.\n---In case of using parameter < * >, every value, stored in every node of DHT, is returned.---\n  
4. DEPART <id>\nIn this option a node with given id,leaves the Net gracefully\n
5. OVERLAY\nIn this option, the topology of connected nodes is printed through their IDs\n  
6. HELP\nPress HELP in order for you to know your options!\n """

    return strr

#-------------------------------------------- IsConnected -----------------------------------------------------------#
'''
This function checks if a socket is already closed
'''

def IsConnected(soc):
    res = str(soc).find('[closed]')
    if res == -1:
        return True
    else:
        return False

#-------------------------------------------- TIME_KEEPER -----------------------------------------------------------#    
'''
In this funtion, in file with name that the argument 'name' has, we r\write the value,saved in variable t1.
'''

def TIME_KEEPER(name,t1):
    
    f = open(name, "a")
    f.write("{}\n".format(t1))
    f.close()
    
#-------------------------------------------- FORWARD_to_SERVERNODE---------------------------------------------------#
'''
This function takes over to tranfer a message from the last Node of the DHT circle to the  Server (first) Node
'''

def FORWARD_to_SERVERNODE(instr, id_,key,value,cli_ip,cli_port,num_k,time,version,where):
    global IsThereConn,Socket2,host_S,port_S
    
    if instr == "INSERT":
        Send_to_Serv = {"req":str(instr), "key":key, "value":str(value),"cli_port":int(cli_port), "cli_ip":str(cli_ip),"num_k":int(num_k),"time":time,"where":where}
    elif instr == "DELETE":
        Send_to_Serv = {"req":str(instr), "key":key, "value":str(value),"cli_port":int(cli_port), "cli_ip":str(cli_ip),"time":time}
    elif instr == "QUERY":
        Send_to_Serv = {"req":str(instr), "key":key,"cli_port":int(cli_port), "cli_ip":str(cli_ip),"num_k":num_k,"time":time,"version":version}
    elif instr == "DEPART":
        Send_to_Serv = {"req":str(instr),"id_given":int(id_)}
    elif instr == "OVERLAY":
        Send_to_Serv = {"req":str(instr),"cli_port":int(cli_port), "cli_ip":str(cli_ip), "time":time}
    elif instr == "ToUpDate":
        Send_to_Serv = {"req":str(instr), "id_from_ns":id_}
    else:
        Send_to_Serv = {"req":str(instr), "id_given":id_}
        
    if IsThereConn >1:
        if IsConnected(Socket2):
            Socket2.close()

    IsThereConn = IsThereConn + 1    
    Socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    Socket2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    Socket2.connect((host_S,port_S))
    Socket2.sendall(pickle.dumps(Send_to_Serv))

#-------------------------------------------- Hashing ---------------------------------------------------#
'''
In this fucntion, SH1 is appropriately called and 'Hashing' returns an integer which is <=1024.
(This happens so as not to have very large numbers)
'''

def Hashing(arg1,arg2):
    if arg2!=None:
        hashId = hashlib.sha1((str(arg1)+str(arg2)).encode()).hexdigest()
        hashId_int = int(hashId,16)
        #return (hashId_int % size) + size
        return hashId_int % size
    else:
        hashId = hashlib.sha1((str(arg1)).encode()).hexdigest()
        hashId_int = int(hashId,16)
        return hashId_int % size 
      
#-------------------------------------------- DEPART ---------------------------------------------------#
'''
In this function, a node takes over to inform the nodes to whom it is connected and then tne node departs greacefully.
Also, the node sends its data to the  next one if exists, else it send them to its previous one
'''

def DEPART(id_,soc):

    global server_NODE_socket,CLI_THREAD,DataList,Replicas
    print("To Depart ...\n")
    
    if NodeInfo["succ"]!=None:
        if DataList:
            Update = {"req_back":"UpdateKeys_D", "listt":DataList}
            NodeInfo["succ"].send(pickle.dumps(Update))
            time.sleep(0.3)
            
        if Replicas:
            Update = {"req_back":"UpdateKeys_R", "listt":Replicas}
            NodeInfo["succ"].send(pickle.dumps(Update))
            time.sleep(0.5)
        
        Send = {"req_back":"iWannaLeave", "id_given":int(id_)}
        NodeInfo["succ"].send(pickle.dumps(Send))
        time.sleep(1)
        
        Send = {"req_back":"ChangeConnection","ip":NodeInfo["ip_pred"], "pred":NodeInfo["pred"], "ToDo":True, "Depart":""}
        NodeInfo["succ"].send(pickle.dumps(Send))
        time.sleep(0.2)
        
        print("\nNode[{}] gracefully leaves the Net ...".format(NodeInfo["id"]))
        soc.close()
     
        server_NODE_socket.close()
        Replay_socket.close()
        sys.exit(0)
           
    else:
        if DataList:
            Update = {"req":"UPDATE_D", "listt":DataList}
            soc.send(pickle.dumps(Update))
            time.sleep(0.3)
            
        if Replicas:    
            Update = {"req":"UPDATE_R", "listt":Replicas}
            soc.send(pickle.dumps(Update))
            time.sleep(0.5)
        
        print("\nNode[{}] gracefully leaves the Net ...".format(NodeInfo["id"]))
        Send = {"req":"TheLastLeaves","id_given":int(id_)}
        soc.send(pickle.dumps(Send))
        FORWARD_to_SERVERNODE("iWannaLeave",int(id_),None,None,None,None,None,None,None,None)
        time.sleep(0.5)
        soc.close()
        Instr_Socket.sendall(pickle.dumps({"exit":"exit"}))
        
        print("\nNode[{}] gracefully leaves the Net ...".format(NodeInfo["id"]))
        
        server_NODE_socket.close()
        Replay_socket.close()
        sys.exit(0)
           
#-------------------------------------------- Node_server_Handler ----------------------------------------------#
'''
This function handles the received message. It runs in a new thread that is activated through the server of this every Node 
'''

def Node_server_Handler(conn,ip,port):
    
    global NodeInfo, listeningPort,listeningPort_ip, Instr_Socket,DataList,Replicas  
    
    while True:
      
      if conn != Instr_Socket:
          time.sleep(0.5)
          rcv = conn.recv(2048)
         
          if rcv:
              print("HELLO! Waiting in Node's[{}] Listening Port {}:{} ".format(NodeInfo["id"],listeningPort_ip,listeningPort))
              data = pickle.loads(rcv)
              
              if  data["req"] == "reConnect":
                 
                  if NodeInfo["succ"]!=None:
                      print("I am about to change connection!")
                      Send = {"req_back":"ChangeConnection", "pred":data["pred"], "ip":data["ip"], "ToDo":False}
                      
                      NodeInfo["succ"].sendall(pickle.dumps(Send))
                      NodeInfo["succ"] = conn
                  else:
                      print("i haven't a connection earlier ...")
              
              elif data["req"] == "hello":
                  print("Welcome NodeId: ",data["id"])
              
              elif data["req"] == "TheLastLeaves":
                  NodeInfo["succ"] = None
                  print("The last one, gone!")
              
              elif data["req"] == "Check_for_Keys":
                      print("Wait for the keys to be updated ...\n")
                    
                      new_list = []
                      for i,item in enumerate(DataList,0): 
                          if item['hash_key'] <= data["id_from_ns"] and item['hash_key'] > NodeInfo["id"] : 
                              new_list.append(item)
                              DataList = [i for i in DataList if not (i["hash_key"] == item['hash_key'])]
                              
                      time.sleep(0.5)
                      if new_list:
                         Send = {"req_back":"UpdateKeys_D", "listt":new_list}      
                         conn.sendall(pickle.dumps(Send))
                      new_list.clear()
                       
                      for i,item in enumerate(Replicas,0):
                          if item['hash_key'] <= data["id_from_ns"]: 
                              new_list.append(item)
                              Replicas = [i for i in Replicas if not (i["hash_key"] == item['hash_key'])]
                              
                      time.sleep(0.5)
                      if new_list:
                         Send = {"req_back":"UpdateKeys_R", "listt":new_list}      
                         conn.sendall(pickle.dumps(Send))
                     
                      new_list.clear()
                      print("keys update: SEND!\n")
                      
              elif data["req"] == "UPDATE_D":  
                      DataList = DataList + data["listt"]
                      DataList = [dict(t) for t in {tuple(d.items()) for d in DataList}]
                      
              elif data["req"] == "UPDATE_R":
                      Replicas = Replicas + data["listt"]
                      Replicas = [dict(t) for t in {tuple(d.items()) for d in Replicas}]
              
              print("SERVER DONE!")        
       
    
#-------------------------------------------- Listen ---------------------------------------------------#
'''
This funtion creates a server, in which this Node can listen requests from the other ones
(regarding the procedure of join, or depart)and the client

'''

def Listen():
  
    global NodeInfo,server_NODE_socket,Instr_Socket,listeningPort,listeningPort_ip
    
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    listeningPort_ip = s.getsockname()[0]
    print("IP SERVER: ",listeningPort_ip)
    s.close()
    
    try:
        server_NODE_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_NODE_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
    except socket.error:
        print("socket error detected!")
        sys.exit(-1)
        
    try:
        # bind socket
        server_NODE_socket.bind((listeningPort_ip, 0))
        listeningPort = server_NODE_socket.getsockname()[1]
    except socket.error:
        print("socket error detected in port:{}!".format(listeningPort))
        sys.exit(-1)

    #listen for incoming connections
    print("[NODE] start listening concurently in {}...".format(listeningPort))
    server_NODE_socket.listen(100)
   
    count = 0
    
    while True:

        try:
            count = count +1
            (conn,(ipConn,portConn)) = server_NODE_socket.accept()
            print("----------------------New Connection!")
            Client_Thread = Thread(name="NodeThread", target=Node_server_Handler, args=(conn,ipConn, portConn))
            if count == 1:
                Instr_Socket = conn
                print("Instruction socket informed")
                
            if count == 2:
                NodeInfo["succ"] = conn

            Client_Thread.daemon = True
            Client_Thread.start()
            THREADS.append(Client_Thread)
            
        except(KeyboardInterrupt):
            print("\n\tkeyBoardInterrupt!!!!")
            Replay_socket.close()
            server_NODE_socket.close()
            sys.exit(-1)

    server_NODE_socket.close()    

#-------------------------------------------- CLIENT_FUNCTION ---------------------------------------------------#
'''
This function is created in order for the client to be able to send requests through every Node.
This function runs into a new thread, that is activated in Main() and is alive during the whole process.
'''

def CLIENT_FUNCTION(host,port):
    
    global LISTENING_REPLAY_IP,LISTENING_REPLAY_PORT,Name_of_file
    
    Client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    Client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    Client_socket.connect((host,port))
    print("CLI in port: {} successfully connected!\n".format(port))
    
    with open(Name_of_file) as f:
      lines = f.read().splitlines()
    
    #start = int(input("Give start number: "))
    #end = int(input("Give end number: "))
    
    while True:
        value = ""
        id_given = 0
        key = ""
        
          
        #given_input = input("Give an order through CLI: ")
        
        #if str(given_input) == "DELETE" or str(given_input) =="QUERY": 
            #key = input("Give a key: ")
           
        #elif str(given_input) == "INSERT":
            #for i in lines[start:end]:
              #press = input("ENTOLH ")
              #if press == "exit":
                #break
              #print("LIST ",i)
              #print("------------------")
              ##data = i.split(",")
              ##print("line 0\n",data[0])
              ##print("line 1\n",data[1])
              ##key = data[0]
              ##value = data[1]
              #key = i
              #Send = {"req_back":str(given_input),"where":-1, "num_k":k_factor,"cli_ip":LISTENING_REPLAY_IP, "cli_port":LISTENING_REPLAY_PORT, "time":-1,"key":key, "id_given":int(id_given),"value":value,"version":""}
              #Client_socket.sendall(pickle.dumps(Send))
          

        #asks for the user to give an order
        given_input = input("Give an order through CLI: ")
        
        if str(given_input) == "DELETE" or str(given_input) =="QUERY" or str(given_input) == "INSERT": 
            key = input("Give a key: ")
            
            if str(given_input) == "INSERT":
                value = input("Give a value: ")
          
        elif str(given_input) == "DEPART":
            id_given = input("Give an id: ")
     
       
        Send = {"req_back":str(given_input),"where":-1, "num_k":k_factor,"cli_ip":LISTENING_REPLAY_IP, "cli_port":LISTENING_REPLAY_PORT, "time":-1,"key":key, "id_given":int(id_given),"value":value,"version":""}
        Client_socket.sendall(pickle.dumps(Send))
        
#-------------------------------------------- REPLAY_2_CLI ---------------------------------------------------#
'''
This function is used in order for the Node which is processing a request, to answer to the Node which made the request.
'''
    
def REPLAY_2_CLI(instr,strr,listt,host,port):
  
    global NodeInfo,s1
    
    if s1!=None:
      if IsConnected(s1):
          s1.close()
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    s1.connect((host,port))
    
    if strr!=None:
        rep = "replay from node {}\n--> {}\n".format(NodeInfo["id"],strr)
        Send = {"req":str(instr),"ans":rep, "listt":None}
        
    elif listt!=None:
        rep = "replay from node {}:\n".format(NodeInfo["id"])
        Send = {"req":str(instr),"ans":rep,"listt":listt}
        
        
    s1.sendall(pickle.dumps(Send))
    

#-------------------------------------------- Listen_Replies ------------------------------------------------#
'''
In this function, a new server is initialized in order to accept the replies on the requests of the other Nodes.
It's Ip and Port are global variables and are passed as arguments through the message that the client sends to the Nodes.
'''

def Listen_Replies():
  
    global Replay_socket,DataList,Replicas,LISTENING_REPLAY_IP,LISTENING_REPLAY_PORT
    
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    LISTENING_REPLAY_IP = s.getsockname()[0]
    s.close()
    
    try:
        Replay_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        Replay_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
    except socket.error:
        print("socket error detected!")
        sys.exit(-1)
        
    try:
        # bind socket
        Replay_socket.bind((LISTENING_REPLAY_IP, 0))
        LISTENING_REPLAY_PORT = Replay_socket.getsockname()[1]
    except socket.error:
        print("socket error detected in port:{}!".format(LISTENING_REPLAY_PORT))
        sys.exit(-1)

    # listen for incoming connections
    print("Start listening for Replies in {}...".format(LISTENING_REPLAY_PORT))
    Replay_socket.listen(100)
  
    
    while True:

        try:
            (conn,(ipConn,portConn)) = Replay_socket.accept()
            print("----------------------New Replay!")
            
            rcv = conn.recv(4096)
            if rcv:
                data = pickle.loads(rcv)
               
                if data["req"] == "ServerAnswered":
                    print(data["ans"])
                    if data["listt"]!=None:
                       for item in data["listt"]: 
                           print(item)

                else:
                    print(data["ans"])
                    if data["listt"]!=None:
                       for i in range(len(data["listt"])): 
                           strr = "hash_key: {}  (key,value) = ({},{})".format(data["listt"][i]["hash_key"],data["listt"][i]["key"],data["listt"][i]["value"])
                           print (i, strr)
                    print("REPLAY: PRINTED!")
                 
            
        except(KeyboardInterrupt):
            print("\n\tkeyBoardInterrupt!!!!")
            Replay_socket.close()
            server_NODE_socket.close()
            sys.exit(-1)

    Replay_socket.close() 

############################################################ -- MAIN() -- ######################################################## 

def Main():

    global NodeInfo,ThereISconnection, SUCC,listeningPort,listeningPort_ip,SERV_Count, IsThereConn,QUERY_DONE,CLI_THREAD
    global DataList,Replicas,SEARCH_DONE,ThereIs,DELETE_DONE,filename,t0,Instr_Socket,host_S,port_S,Name_of_file

    socks = []
    
    #asks for the ip and port in which is going to connect
    HostPort = str(input("\nGive Host Address/Port Number\n"))
    HostPort_edited= HostPort.split("/")
    host_S = HostPort_edited[0]
    port_S = int(HostPort_edited[1])
    
    #make the Server thread (for replies) to run
    RECEIVER = Thread(name="RECEIVER", target=Listen_Replies)
    RECEIVER.daemon = True
    RECEIVER.start()
    
    THREADS.append(RECEIVER)
    time.sleep(0.5)
    
    #make the Server thread(for communication between the Nodes and for the client) to run
    Listening = Thread(name="ServerThreadNode", target=Listen)
    Listening.daemon = True
    Listening.start()
    
    THREADS.append(Listening)
    
    
    time.sleep(0.1)
    
     #make the Client thread to run (it connects to the 'ServerThreadNode')  
    CLI_THREAD = Thread(name="CLI", target=CLIENT_FUNCTION, args=(listeningPort_ip,int(listeningPort)))
    CLI_THREAD.daemon = True
    CLI_THREAD.start()
    
    THREADS.append(CLI_THREAD)
    
    time.sleep(0.2)
    
    
    #connection to 1st node    
    Socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    Socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    Socket.connect((host_S,port_S))
    
    Send = {"req":"join", "ListeningPort":listeningPort, "ip":listeningPort_ip}
    Socket.sendall(pickle.dumps(Send))
    print("\nWaiting for the server to join me ...")
    
    socks = [Instr_Socket,Socket]
   
    while True:
        socks = [Instr_Socket,Socket] #a list of sockets from which this Node can receive a message
        ready_socks ,rw , rxl = select.select(socks,[],[]) #chooses the socket that is ready for reading it  
        
        for sock in ready_socks:
           
            rcv = sock.recv(2048)
           
            if rcv:
                msg = pickle.loads(rcv) #pickle: decodes the received message
              
                #after decoding the message, we check which option we have to execute
                if msg["req_back"] == "Joined":
                
                    NodeInfo["id"] = int(msg["id"])
                    NodeInfo["number"] = msg["counter"]
                    
                    NodeInfo["pred"] = port_S
                    NodeInfo["ip_pred"] = host_S
                    
                    print("I've joined the NET!\nid: ",NodeInfo["id"])
                    print("number = ", NodeInfo["number"])
                    
                elif msg["req_back"] == "ChangeConnection":
                   
                    Socket.close() #close the previous connection
                    Socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    Socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    
                    #change the connection of the Node in order to be connected to the write one
                    Socket.connect((msg["ip"],msg["pred"]))
                    NodeInfo["pred"] = msg["pred"]
                    NodeInfo["ip_pred"] = msg["ip"]
                    
                    print("--------- Reconnection Done from Node:  ----------------",NodeInfo["id"])
                    
                    #if 'ToDo' is true, it means that the new Node has to be put in the middle of 2 others so, the previous Node has to appropriately inform its old next one,to accept the changes.
                    if msg["ToDo"] == True:
                      
                        Send = {"req":"reConnect", "pred":listeningPort,"ip":listeningPort_ip, "id":NodeInfo["id"]}
                        Socket.sendall(pickle.dumps(Send))
                        
                      
                    else:
                        Send = {"req":"hello","id":NodeInfo["id"]}
                        Socket.sendall(pickle.dumps(Send))
                        
                 #in this option,every node checks if any keys have to be saved in other nodes
                elif msg["req_back"] == "ToUpDate": 
   
                    if NodeInfo["id"] == msg["id_from_ns"]:
                        print("Checking for key updates ...")
                        
                        Send = {"req":"Check_for_Keys","req_back":"Check_for_Keys","id_from_ns":msg["id_from_ns"]}
                        
                        if NodeInfo["succ"]!=None:
                           NodeInfo["succ"].sendall(pickle.dumps(Send))
                           time.sleep(0.5)
                           Socket.sendall(pickle.dumps(Send)) 
                        else:
                           Socket.sendall(pickle.dumps(Send))
                                      
                    else:
                        if NodeInfo["succ"]!=None:
                          NodeInfo["succ"].sendall(pickle.dumps(msg))
                        else:
                          FORWARD_to_SERVERNODE("ToUpDate",msg["id_from_ns"],None,None,None,None,None,None,None,None) 
                
                #procedure of insertion 
                elif msg["req_back"] == "INSERT":
                    time.sleep(0.3)
                    flag = False
                    
                    id_new = int(Hashing(str(msg["key"]),None))
                    print("key given: ", msg["key"])
                    print("Hash key = ",id_new)

                    
                    if msg["time"] == -1:
                        t0 = time.perf_counter()
                        msg["time"] = datetime.timestamp(datetime.now())
                        SEARCH_DONE = msg["time"]

                        
                        if NodeInfo["succ"] != None:
                            NodeInfo["succ"].sendall(pickle.dumps(msg)) 
                        else:
                            print("Forward the message ...")
                            FORWARD_to_SERVERNODE("INSERT",None,msg["key"],msg["value"],msg["cli_ip"],msg["cli_port"],msg["num_k"],msg["time"],None,msg["where"]) 
                     
                    elif SEARCH_DONE != msg["time"] and msg["time"]!=1: 

                        
                        if NodeInfo["succ"] != None:
                            NodeInfo["succ"].sendall(pickle.dumps(msg)) 
                        else:
                            print("Forward the message ...")
                            FORWARD_to_SERVERNODE("INSERT",None,msg["key"],msg["value"],msg["cli_ip"],msg["cli_port"],msg["num_k"],msg["time"],None,msg["where"]) 
                        
                    elif SEARCH_DONE == msg["time"]:
                        print("I found the first Node!\n")
                        msg["time"] = 1
   
                        
                    if msg['time'] == 1:
                        print("where to be saved: ",msg["where"])
                        #if id_new <= NodeInfo["id"]:
                        
                        if NodeInfo["id"] == msg["where"]:
                            
                            #Replicas = [i for i in Replicas if not (i["hash_key"] == id_new)]
                            DataList = [i for i in DataList if not (i["hash_key"] == id_new)]
                            
                            n = {"hash_key":id_new, "key":msg["key"],"value":msg["value"]} 
                            DataList.append(n)
                            msg["num_k"] = msg["num_k"] -1       
                            REPLAY_2_CLI("","New Info: Inserted!",None,msg["cli_ip"],msg["cli_port"])
                           
                        elif msg["num_k"] > 0 and msg["num_k"] != k_factor:
                            
                            Replicas = [i for i in Replicas if not (i["hash_key"] == id_new)]
                            #DataList = [i for i in DataList if not (i["hash_key"] == id_new)]
                            
                            n = {"hash_key":id_new,"key":msg["key"], "value":msg["value"]}
                            Replicas.append(n)
                            msg["num_k"] = msg["num_k"] -1
                            print("Replicated: num_k = ", msg["num_k"])
                            
                            
                        if msg["num_k"] == 0:
                               t1 = time.perf_counter() -t0
                               TIME_KEEPER(filename,t1)    
                               #REPLAY_2_CLI("","New Info: Inserted!",None,msg["cli_ip"],msg["cli_port"]) 
                         
                        if msg["num_k"] > 0:
                            if NodeInfo["succ"] != None:
                                NodeInfo["succ"].sendall(pickle.dumps(msg)) 
                            else:
                                print("Forwarding the message ...")
                                FORWARD_to_SERVERNODE("INSERT",None,msg["key"],msg["value"],msg["cli_ip"],msg["cli_port"],msg["num_k"],msg["time"],None,msg["where"])
                     
                 #procedure of delete            
                elif msg["req_back"] == "DELETE":
                  
                    time.sleep(0.3)
                    id_new = int(Hashing(msg["key"],None))
                    print("**** DELETE ****")
                    if msg["time"] == -1:
                      
                        DELETE_DONE = True
                        msg["time"] = 0
                        
                        Replicas = [i for i in Replicas if not (i["hash_key"] == id_new)]
                        DataList = [i for i in DataList if not (i["hash_key"] == id_new)]

                        
                        if NodeInfo["succ"] != None:
                            Send = {"req_back":"DELETE","cli_ip":msg["cli_ip"], "cli_port":msg["cli_port"], "key":msg["key"], "value":msg["value"],"time":msg["time"]}
                            NodeInfo["succ"].sendall(pickle.dumps(Send)) 
                        else:
                            print("Forward the message ...")
                            FORWARD_to_SERVERNODE("DELETE",None,msg["key"],msg["value"],msg["cli_ip"],msg["cli_port"],None,msg["time"],None,None)

                        
                    elif DELETE_DONE == False:
                        
                        Replicas = [i for i in Replicas if not (i["hash_key"] == id_new)]
                        DataList = [i for i in DataList if not (i["hash_key"] == id_new)]
                        
                        if NodeInfo["succ"] != None:
                            Send = {"req_back":"DELETE","cli_ip":msg["cli_ip"], "cli_port":msg["cli_port"], "key":msg["key"], "value":msg["value"],"time":msg["time"]}
                            NodeInfo["succ"].sendall(pickle.dumps(Send)) 
                        else:
                            print("Forwarding the message ...")
                            FORWARD_to_SERVERNODE("DELETE",None,msg["key"],msg["value"],msg["cli_ip"],msg["cli_port"],None,msg["time"],None,None)
                            
                    elif DELETE_DONE:
                         
                         DELETE_DONE = False
                         REPLAY_2_CLI("","Deleting Procedure ... Done!",None,msg["cli_ip"],msg["cli_port"])
      
                 #procedure of query        
                elif msg["req_back"] == "QUERY":
                   
                    time.sleep(1)
                    total =[]
                   
                    flag_D = False
                    flag_R = False
                    id_new = int(Hashing(str(msg["key"]),None))
                  
                    if str(msg["key"]) == "*":
                    
                        if msg["time"] == -1:
                            QUERY_DONE = True
                            t0 = time.perf_counter()
                           
                            total = DataList + Replicas
                            total = [dict(t) for t in {tuple(d.items()) for d in total}]
                            REPLAY_2_CLI("REPLAY",None,total,msg["cli_ip"], msg["cli_port"])
                            
                            t1 = time.perf_counter() - t0
                            nameF = "Eventual_QUERY_k="+str(k_factor)+"_"+Name_of_file.split(".")[0]+".txt"
                            TIME_KEEPER(nameF,t1)
                            
                            total.clear()
                            
                            msg["time"] = 0
                            if NodeInfo["succ"]!=None:
                                Send = {"req_back":"QUERY", "key":msg["key"],"time":msg["time"], "cli_ip":msg["cli_ip"],"cli_port":msg["cli_port"]}
                                NodeInfo["succ"].sendall(pickle.dumps(Send)) 
                            else:
                                FORWARD_to_SERVERNODE("QUERY",None, msg["key"],None,msg["cli_ip"],msg["cli_port"],None,msg["time"],"",None)
                        
                        elif QUERY_DONE != True:
                          
                            
                            total = DataList + Replicas
                            total = [dict(t) for t in {tuple(d.items()) for d in total}]
                            REPLAY_2_CLI("REPLAY",None,total,msg["cli_ip"], msg["cli_port"])
                            
                            t1 = time.perf_counter() - t0
                            nameF = "Eventual_QUERY_k="+str(k_factor)+"_"+Name_of_file.split(".")[0]+".txt"
                            TIME_KEEPER(nameF,t1)
                            
                            total.clear()
                            
                            if NodeInfo["succ"]!=None:
                                Send = {"req_back":"QUERY", "key":msg["key"],"time":msg["time"], "cli_ip":msg["cli_ip"],"cli_port":msg["cli_port"]}
                                NodeInfo["succ"].sendall(pickle.dumps(Send)) 
                            else:
                                FORWARD_to_SERVERNODE("QUERY",None, msg["key"],None,msg["cli_ip"],msg["cli_port"],None,msg["time"],"",None)
                        
                        elif QUERY_DONE == True:
                            print("I found the first NODE!")
                            QUERY_DONE = False
                            
                    else:     
                        if id_new <= NodeInfo["id"]:
                            for i in range(len(DataList)): 
                                if DataList[i]["hash_key"] == id_new:
                                    rep = "(key,value) = ({},{})\n".format(DataList[i]["key"],DataList[i]["value"])
                                    REPLAY_2_CLI("REPLAY",rep,None,msg["cli_ip"], msg["cli_port"])
                                    
                                    t1 = time.perf_counter() - t0
                                    nameF = "Eventual_QUERY_k="+str(k_factor)+"_"+Name_of_file.split(".")[0]+".txt"
                                    TIME_KEEPER(nameF,t1)
                                    
                                    rep = " (key,value) = ({},{})\n".format(DataList[i]["key"],DataList[i]["value"])
                                    TIME_KEEPER("Queries_Eventual_exp_3",rep)
                                    
                                    total.clear()
                            
                                    flag_D = True
                                    
                        if flag_D == False:
                            for i in range(len(Replicas)): 
                                if Replicas[i]["hash_key"] == id_new: 
                                    rep = "(key,value) = ({},{})\n".format(Replicas[i]["key"],Replicas[i]["value"])
                                    REPLAY_2_CLI("REPLAY",rep,None,msg["cli_ip"], msg["cli_port"])
                                    
                                    t1 = time.perf_counter() - t0
                                    nameF = "Eventual_QUERY_k="+str(k_factor)+"_"+Name_of_file.split(".")[0]+".txt"
                                    TIME_KEEPER(nameF,t1)
                                    
                                    rep = " (key,value) = ({},{})\n".format(Replicas[i]["key"],Replicas[i]["value"])
                                    TIME_KEEPER("Queries_Eventual_exp_3",rep)
                                    
                                    total.clear()
                            
                                    flag_R = True
                        
                        if flag_D == False and flag_R == False:
                          
                            if NodeInfo["succ"]!=None:
                              Send = {"req_back":"QUERY", "num_k":msg["num_k"],"key":msg["key"],"time":msg["time"],"version":msg["version"],"cli_ip":msg["cli_ip"],"cli_port":msg["cli_port"]}
                              NodeInfo["succ"].sendall(pickle.dumps(Send)) 
                            else:
                              FORWARD_to_SERVERNODE("QUERY",None, msg["key"],None,msg["cli_ip"],msg["cli_port"],msg["num_k"],msg["time"],msg["version"],None)
                    
                 #procedure of depart    
                elif msg["req_back"] == "DEPART":
                    time.sleep(0.3)
                
                    print("id_given: ",msg["id_given"])
                    
                    if NodeInfo["id"] == int(msg["id_given"]):
                        DEPART(int(msg["id_given"]),Socket)
                    
                        
                    else:
                        print("forwardig departing ...")
                        if NodeInfo["succ"]!=None:
                          Send = {"req_back":"DEPART","id_given":int(msg["id_given"])}
                          NodeInfo["succ"].sendall(pickle.dumps(Send)) 
                        else:
                          FORWARD_to_SERVERNODE("DEPART",msg["id_given"],None,None,None,None,None,None,None,None)
                          
                         
                elif msg["req_back"] == "iWannaLeave": #msg to servernode in order to delete my id when i try to leave
                    
                      if NodeInfo["succ"] != None:
                        
                          Send = {"req_back":"iWannaLeave","id_given":int(msg["id_given"]) }
                          NodeInfo["succ"].sendall(pickle.dumps(msg)) 
                      else:
                          FORWARD_to_SERVERNODE("iWannaLeave", int(msg["id_given"]),None,None,None,None,None,None,None,None)
                
                 #check if there are keys that have to be saved in other Nodes    
                elif msg["req_back"] == "Check_for_Keys":
                  
                    print("Wait for the keys to be updated ...\n")
                    
                    new_list = []
                    for i,item in enumerate(DataList,0): 
                        if item['hash_key'] <= msg["id_from_ns"]: 
                            new_list.append(item)
                            DataList = [i for i in DataList if not (i["hash_key"] == item['hash_key'])]
                            
                    time.sleep(1)        
                    Send = {"req":"UPDATE_D","listt":new_list}
                    sock.sendall(pickle.dumps(Send))
                    new_list.clear()

                    for i,item in enumerate(Replicas,0): 
                        if item['hash_key'] <= msg["id_from_ns"]: 
                            new_list.append(item)
                            Replicas = [i for i in Replicas if not (i["hash_key"] == item['hash_key'])]
                    
                    time.sleep(1) 
                    Send = {"req":"UPDATE_R","listt":new_list}
                    sock.sendall(pickle.dumps(Send))
                    
                    new_list.clear()
                    print("keys update: DONE!\n")
                    sock.sendall(pickle.dumps(Send))                    
                
                #procedure of updating my lists in case an other node had a key that belongs to me
                elif msg["req_back"] == "UpdateKeys_D":
                    DataList = DataList + msg["listt"]
                    DataList = [dict(t) for t in {tuple(d.items()) for d in DataList}]
                    
                elif msg["req_back"] == "UpdateKeys_R":
                    Replicas = Replicas + msg["listt"]
                    Replicas = [dict(t) for t in {tuple(d.items()) for d in Replicas}]
                    
                    print("List: updated!")
                    
                #procedure of overlay    
                elif msg["req_back"] == "OVERLAY":
                    time.sleep(0.3)
                    
                    #forward the message to the next Node until i find the Server Node
                    if NodeInfo["succ"] != None:
                        
                          Send = {"req_back":"OVERLAY","cli_ip":msg["cli_ip"],"cli_port":msg["cli_port"]}
                          NodeInfo["succ"].sendall(pickle.dumps(msg)) 
                    else:
                          FORWARD_to_SERVERNODE("OVERLAY",None,None,None,msg["cli_ip"],msg["cli_port"],None,None,None,None)
                          
                #procedure of help          
                elif msg["req_back"] == "HELP":
                   
                    REPLAY_2_CLI("REPLAY",HELP_INFO(),None,msg["cli_ip"], msg["cli_port"])
                    
                  
                print("DONE!")  
                    
    for t in THREADS:
        t.join()
    
    Socket.close()

if __name__ == "__main__":
    Main()









