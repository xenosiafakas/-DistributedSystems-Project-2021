#                                                                                                                 #
#                                   This code is the client of the Server Node                                    #
#                                                                                                                 #
###################################################################################################################
import socket
import signal 
from threading import Thread 
import sys
import pickle
import threading
import signal
import hashlib
import errno
import fcntl,os
from time import sleep
import time
from datetime import datetime
#-------------------------------------- Global Variables -----------------------------------------------------------#
Name_of_file = "query.txt"
k_factor = 2
Cli_ip = ""
Cli_port = 0
COUNTER = 1
########################################### - FUNCTIONS - ############################################################

#---------------------------------------------- Node_Handler --------------------------------------------------------#
'''
 This function handles the incoming request, which constists of messages that have to be printed.
'''
def Node_Handler(conn,ip,port):
    while True:
      
        sleep(0.5)
      
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
                    
           print("REPLAY: PRINTED!\n")
                 
#-------------------------------------------- Listen ---------------------------------------------------------------#

def Listen():
    
    global Cli_ip,Cli_port,COUNTER
    
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    Cli_ip = s.getsockname()[0]
    s.close()
    
    try:
        server_NODE_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_NODE_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
    except socket.error:
        print("socket error detected!")
        sys.exit(-1)


    try:
        # bind socket
        server_NODE_socket.bind((Cli_ip, 0))
        Cli_port = server_NODE_socket.getsockname()[1]
    except socket.error:
        print("socket error detected in {}:{}!".format(Cli_ip,Cli_port))
        sys.exit(-1)

    # listen for incoming connections
    print("[NODE] start listening in {}:{}...".format(Cli_ip,Cli_port))
    server_NODE_socket.listen(100)
   
        
    while True:

        try:
            
            (conn,(ipConn,portConn)) = server_NODE_socket.accept() #accept the incoming request
            print("\n---------------------- New Replay!\n")
            COUNTER = COUNTER + 1
            #after accepting the request, the function Node_Handler is running through a new thread
            Client_Thread = Thread(name="NodeThread", target=Node_Handler, args=(conn,ipConn, portConn))
            
            Client_Thread.daemon = True
            Client_Thread.start()
          
        except(KeyboardInterrupt):
            print("\n\tkeyBoardInterrupt!!!!")
            server_NODE_socket.close()
            sys.exit(-1)

    server_NODE_socket.close()  

########################################### - MAIN() - ############################################################


def Main():

    global Cli_port,Cli_ip,Name_of_file,COUNTER
    
    #asks for the details inorder to connect to Server Node
    HostPort = str(input("\nGive Host Address/Port Number\n"))
    HostPort_edited = HostPort.split("/")
    host = HostPort_edited[0]
    port = int(HostPort_edited[1])
    
    #the thread for listening replies is now running
    Listening = Thread(name="CLI_NODE", target=Listen)
    Listening.daemon = True
    Listening.start()
   
    sleep(0.3)
    
    print("[NODE] start listening, concurently in: {}:{} ...\n".format(Cli_ip,Cli_port))
    
    #connect to the Server Node
    Client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    Client_socket.connect((host,port))
    
    
     
    with open(Name_of_file) as f:
      lines = f.read().splitlines()
    
    start = int(input("Give start number: "))
    end = int(input("Give end number: "))
    
    
    
    while True:
       
        value = ""
        id_given = 0
        key = ""
        
######################################## if the orders are given through files #########################################
        if Name_of_file == "requests.txt":
            if start <= end and start!=COUNTER:
            
                data = lines[start].split(",")
              
                if str(data[0]) =="insert":
                    given_input = "INSERT"
                else:
                    given_input = "QUERY"
                    
                key = str(data[1])
              
                if len(data) == 3:
                    value = data[2]
                  
                print(start, " ENTOLH - KEY - VALUE: ",data)
              
              
                Send = {"req":str(given_input), "num_k":k_factor,"cli_ip":Cli_ip, "cli_port":Cli_port, "time":-1,"key":key, "id_given":int(id_given),"value":value,"version":""}
                Client_socket.sendall(pickle.dumps(Send))

                start = start + 1
                COUNTER = start
                print("wait ...")
                time.sleep(0.1)
                
        elif Name_of_file == "query.txt":
        
            if start <= end and start!=COUNTER:
                
                key = str(lines[start])
        
                print(start, " ENTOLH - KEY - VALUE: ",key)
              
              
                Send = {"req":"QUERY", "num_k":k_factor,"cli_ip":Cli_ip, "cli_port":Cli_port, "time":-1,"key":key, "id_given":int(id_given),"value":value,"version":""}
                Client_socket.sendall(pickle.dumps(Send))
                
                start = start + 1
                COUNTER = start
                print("wait ...")
                time.sleep(0.1)
                
        else:
             if start <= end and start!=COUNTER:
               
                data = lines[start].split(",")
                key = str(data[0])
                value = str(data[1])
                
                Send = {"req":"INSERT", "num_k":k_factor,"cli_ip":Cli_ip, "cli_port":Cli_port, "time":-1,"key":key, "id_given":int(id_given),"value":value,"version":""}
                Client_socket.sendall(pickle.dumps(Send))

                start = start + 1
                COUNTER = start
                print("wait ...")
                time.sleep(0.1)

          
          
######################################## if the orders are given through the user #########################################


        #given_input = input("Give an order through CLI: \n")
        
        #if str(given_input) == "DELETE" or str(given_input) =="QUERY" or str(given_input) == "INSERT":
            #key = input("Give a key: ")
            
            #if str(given_input) == "INSERT":
                #value = input("Give a value: ")
           
        #elif str(given_input) == "DEPART":
            #id_given = input("Give an id: ")
        
        #Send = {"req":str(given_input), "num_k":k_factor,"cli_ip":Cli_ip, "cli_port":Cli_port, "time":-1,"key":key, "id_given":int(id_given),"value":value,"version":""}
        #Client_socket.sendall(pickle.dumps(Send))
        
        sleep(0.2)
        
    Client_socket.close()
    
    
 
if __name__ == "__main__":
    Main()









