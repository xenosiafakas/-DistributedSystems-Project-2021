import socket 
from threading import Thread 
import sys
import pickle
import threading
import signal
import hashlib
import time
import fcntl, os
import errno
from time import sleep
import traceback as t

#-------------------------------------------- Global Variables ------------------------------------------------#

m = 10 #number of bits per key
size = 2 ** m #How many keys can i have
conn_ = None
CLI_conn = None
INFO = [] #info for every node that firstly sends 'Join'
ID_server = None 
counter = 0 #current threads
s1 = None
threads = []

###################################### - FUNCTIONS - ########################################################

#---------------------------------------- IsConnected -------------------------------------------------------#
'''
This function checks if a socket is already closed
'''

def IsConnected(soc):
    res = str(soc).find('[closed]')
    if res == -1:
        return True
    else:
        return False

#--------------------------------------- Hashing ------------------------------------------------------------#
'''
In this fucntion, SH1 is appropriately called and 'Hashing' returns an integer which is <=1024.
(This happens so as not to have very large numbers)
'''

def Hashing(arg1,arg2):
    if arg2!=None:
        hashId = hashlib.sha1((str(arg1)+str(arg2)).encode()).hexdigest()
        hashId_int = int(hashId,16)
        return (hashId_int % size)
    else:
        hashId = hashlib.sha1((str(arg1)).encode()).hexdigest()
        hashId_int = int(hashId,16)
        return hashId_int % size 

#---------------------------------------- HELP_INFO -----------------------------------------------------------#
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

#-------------------------------------- REPLAY_2_CLI ----------------------------------------------------------#
'''
This function is used in order for the Node which is processing a request, to answer to the Node which made the request.
'''

def REPLAY_2_CLI(strr,listt,host,port):
  
    global ID_server,s1
    
    if s1!=None:
      if IsConnected(s1):
          s1.close()
    s1 = None
          
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    s1.connect((host,port))
    
    if strr!=None:
        rep = "replay from server node:\n{}".format(strr)
        Send = {"req":"ServerAnswered","ans":rep, "listt":None}
        
    elif listt!=None:
        rep = "replay from server node\n"
        Send = {"req":"ServerAnswered","ans":rep,"listt":listt}
    
    s1.sendall(pickle.dumps(Send))

#----------------------------------------- WRITE_in_TXT ------------------------------------------------------#
'''
This function writes data, saved to variable 'info', into a file with name, saved in variable 'name'
'''
def WRITE_in_TXT(name,info):
    
    f = open(name, "w")
    f.write("{}\n".format(info))
    f.close()


#----------------------------------------- SEARCHING ----------------------------------------------------------#

def SEARCHING(idd):
  global INFO
  ii = 0
  for i in range(len(INFO)):
    ii = ii +1
    if int(idd) <= int(INFO[i]["id"]):
      print("id found: ",int(INFO[i]["id"]))
      return int(INFO[i]["id"])
    
  return INFO[ii-1]["id"]


#---------------------------------------- Incoming_Request_Handler -------------------------------------------------------#
'''
This function handles the incoming requests and forwards the messages to the first Node of list INFO
(this Node is the one that is connected to Server Node)
'''

def Incoming_Request_Handler(conn,ip,port):
    
    global INFO,counter,CLI_conn, conn_,threads,D,R
    
    pos = -1
    while True:
      
        try:
           rcv = conn.recv(4096)
        except:
            print("AN ERROR OCCURED!")
            t.print_exc()
            
        if rcv:
                data = pickle.loads(rcv)
                
                print ("server Process received data from  {}:{}".format(ip,port), data["req"])
                
                #if message is 'join', then the Server Node adds this new Node to the Server Node's list and
                #informs the Nodes, affected by these change
                if data["req"] == "join":
                
                   counter = counter +1
                   print("JOIN!\n")
                   print("COUNTER =", counter)
                   
                   if counter == 1:
                        
                        #creates the dictionary for every new Node
                        new = {"id":Hashing(ip,port), "ip":data["ip"], "Listening_Port":data["ListeningPort"]} 
                        INFO.append(new)#adds the previous dictionary to the list
                        print("1st to join with id: ", INFO[0]["id"])
                        Send = {"req_back":"Joined","id":Hashing(ip,port),"counter":counter}
                        conn.sendall(pickle.dumps(Send)) #send the id to the Node
                        conn_ = conn

                        sleep(0.3)
                           
                        print("ServerNode Process: DONE!")
                          
                   else:
                        new = {"id":Hashing(ip,port), "ip":data["ip"], "Listening_Port":data["ListeningPort"]} 
                        INFO.append(new)
                      
                        Send = {"req_back":"Joined","id":Hashing(ip,port),"counter":counter}
                        conn.sendall(pickle.dumps(Send))
                        
                        sleep(0.2)
                        
                        #sorts the list, in orde for the Nodes to be in the right position
                        INFO = sorted(INFO, key = lambda i: i['id'])
                        print("Wait for the new Node to take the right position ...\n")
                        for item in INFO:
                            print(item,"\n")
                          
                        for i,items in enumerate(INFO,0):
                          if i<= len(INFO):
                              if items["id"] < Hashing(ip,port):
                                  pos = i
                                  
                        #after sorting the list,informs the Nodes that are affected
                        if pos == -1: #I am the first in the DHT
                          
                           Send = {"req_back":"ChangeConnection","ip":INFO[0]["ip"],"pred": INFO[0]["Listening_Port"], "ToDo":False}
                           if conn_!= None:
                              conn_.sendall(pickle.dumps(Send))
                              conn_ = conn
                              
                        elif pos + 2 == len(INFO):#I am the last in the DHT
                            
                            Send = {"req_back":"ChangeConnection","IsLast":False,"ToDo":False, "ip":INFO[pos]["ip"], "pred":int(INFO[pos]["Listening_Port"]), "succ_node":int(INFO[pos+1]["Listening_Port"])}
                            conn.sendall(pickle.dumps(Send))
                        else:
                            #I am in the middle
                            Send = {"req_back":"ChangeConnection","IsLast":False, "ToDo":True, "ip":INFO[pos]["ip"], "pred":int(INFO[pos]["Listening_Port"]), "succ_node":int(INFO[pos+1]["Listening_Port"])}
                            conn.sendall(pickle.dumps(Send))
                        
                        sleep(3)
                        pos = -1
             
                        Send = {"req_back":"ToUpDate", "id_from_ns":int(new["id"])}
                        conn_.sendall(pickle.dumps(Send))
                        sleep(1)
                        print("ServerNode Process: DONE!")
                
                #forwards the message to the next Node, until for the request to be handled
                elif data["req"] == "INSERT":
                    
                    where_to_insert = SEARCHING(Hashing(str(data["key"]),None))
                    
                    print("hash key: ",Hashing(str(data["key"]),None))
                    print("where to save:",where_to_insert)
                    
                    Send = {"req_back":"INSERT","num_k":data["num_k"],"cli_ip":data["cli_ip"], "cli_port":data["cli_port"], "key":data["key"], "value":data["value"],"time":data["time"],"where":where_to_insert}
                    conn_.sendall(pickle.dumps(Send))
                    
                    sleep(0.4)
                    
                  #forwards the message to the next Node, until for the request to be handled
                elif data["req"] == "DELETE":
                     
                    Send = {"req_back":"DELETE","cli_ip":data["cli_ip"], "cli_port":data["cli_port"], "key":data["key"], "value":data["value"],"time":data["time"]}
                    conn_.sendall(pickle.dumps(Send))
                    sleep(0.2)
                    
                    
                elif data["req"] == "reConnect": #when a node departs and another one takes its place
                  
                    print("to rejoin after depart...")
                    conn_ = conn
                    conn_.sendall(pickle.dumps({"req_back":"hiiii"}))
                    sleep(0.2)
                    
                  #forwards the message to the next Node, until for the request to be handled    
                elif data["req"] == "DEPART":
                  
                    print("id_given: ", data["id_given"])
                    Send = {"req_back":"DEPART", "id_given":data["id_given"]}
                    conn_.sendall(pickle.dumps(Send))
                   
                    sleep(0.2)
                    
                elif data["req"] == "iWannaLeave": #inform the DHT when i leave...
                    
                    
                    print("time for id = {} to officially depart ...".format(data["id_given"]))
                    INFO = [i for i in INFO if not (i["id"] == data["id_given"])] #delete the id of the Node that leaves
                    
                    counter = counter - 1 
                    print("Node {} successfully Removed!".format(data["id_given"]))
                    
                #sends all Nodes to the client that made the request    
                elif data["req"] == "OVERLAY":
                
                    print("I'm about to run: overlay ...")
                    Topology_ids = []
                    
                    i = 0
                    for items in INFO:
                        Topology_ids.append("Node [{}] ID: {}".format(i,str(items["id"])))
                        print(Topology_ids[i])
                        i = i+1
                    
                    
                    REPLAY_2_CLI(None,Topology_ids,data["cli_ip"],data["cli_port"])
                    sleep(0.2)
                    
                    Topology_ids.clear()
                    
                  #forwards the message to the next Node, until for the request to be handled    
                elif data["req"] == "QUERY":
                    
                    Send = {"req_back":"QUERY","cli_ip":data["cli_ip"], "cli_port":data["cli_port"], "key":data["key"],"num_k":data["num_k"],"time":data["time"],"version":data["version"]}                    
                    conn_.sendall(pickle.dumps(Send))
                    
                #sends help message to the client that made the request     
                elif data["req"] == "HELP":
                
                    REPLAY_2_CLI(HELP_INFO(),None,data["cli_ip"],data["cli_port"])
                    
              
################################################# -- MAIN() -- #############################################################     

def Main():
    global INFO,counter,ID_server,threads
    
    # create socket for connection
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    host = s.getsockname()[0]
    s.close()
    
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
    except socket.error:
        print("Socket Error Detected!")
        sys.exit(-1)


    try:
        # bind socket
        server_socket.bind( (host, 0) )
        port = int(server_socket.getsockname()[1])
        
    except socket.error:
        print("socket error detected in port:{}!".format(port))
        sys.exit(-1)
    
    
    ID_server = Hashing(host,port)
   
    # listen for incoming connections
    server_socket.listen(100)
    print("\n[SERVER] start listening ...\n\nHost ip/Host port:  {}/{}\n".format(host,port))
    
    info = "\nHost ip/Host port:  {}/{}\n".format(host,port)
    
    WRITE_in_TXT("IP_PORT_Config.txt",info) #write connection details to a .txt file

    while True:

        try:
            
            print("Waiting for a new connection...")
            (conn,(ipConn,portConn)) = server_socket.accept() #accepts new connections
            print("NEW CONNECTION!")
            #handles request through a new Thread
            SER_THREAD = Thread(name="SER_Thread", target=Incoming_Request_Handler, args=(conn,ipConn, portConn))
            
            SER_THREAD.daemon = True
            SER_THREAD.start()
            threads.append(SER_THREAD)
            
        except(KeyboardInterrupt):
            print("\nkeyBoardInterrypt!!!!")
            server_socket.close()
            sys.exit(-1)
    
    for t in threads:
        t.join() #kill all thtreads after doing their job
        
    server_socket.close()    
        


if __name__ == "__main__":
    Main()

