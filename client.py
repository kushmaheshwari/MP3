import sys
from threading import Thread, Lock
import socket
import pickle
import datetime
import time
import random
import signal
import Queue

# Dictionary of port and socket for each node
node_sockets = {}

mutex = Lock()

# Ran with commands "python client.py"
def main():
    parse_file()

    client_thread = Thread(target=setup_client, args = ())
    client_thread.daemon = True
    client_thread.start()

    while True:
        time.sleep(100)

'''
Sets up the client for reading input from the command line
And sets up node 0 in the server
'''
def setup_client():
    # Create node 0
    create_node = Thread(target=setup_node, args = (0,port))#creating node 0
    create_node.daemon = True
    create_node.start()

    while(clientToNode(0, port) == False):
        print("Trying to connect to node")
    while True:
        user_input = raw_input('')
        # Check if a valid command
        if (user_input):
            input_split = user_input.split()
            if (len(input_split) > 1):
                isDigit = False;
                if(input_split[1].isdigit()):
                    nodenum = input_split[1]
                    isDigit = True;

                # Joins a node to the chord
                if(input_split[0] == "join" and isDigit):
                    newport = port + int(input_split[1])
                    create_node = Thread(target=setup_node, args = (input_split[1],newport))
                    create_node.daemon = True
                    create_node.start()

                    while(clientToNode(input_split[1], int(newport)) == False):
                        print("Trying to connect to node " + input_split[1])

                # Finds where a key is stored
                elif(len(input_split) > 2 and input_split[0] == "find" and isDigit and input_split[2].isdigit()):
                    keynum = int(input_split[2])

                # Clean crashes a node
                elif(input_split[0] == "crash" and isDigit):
                    print("Crashing node " + input_split[1])

                # Shows a node's information
                elif(input_split[0] == "show" and input_split[1].isdigit()):
                    print("Showing node ")
                    socket = node_sockets[int(input_split[1])][1]
                    msg = {
                        'source': "client",
                        'message' : "show",
                    }
                    serialized_message = pickle.dumps(msg, -1)
                    socket.sendall(serialized_message)



                # Show all nodes' information
                elif(input_split[0] == "show" and input_split[1] == "all"):
                    print("Showing all nodes info")

                else:
                    print("Invalid Command")

'''
Client connecting to every node that is created
'''
def clientToNode(num, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.connect(("127.0.0.1", port))
        node_sockets[num] = (port,s)
        print("Connected to node " + str(num))
        return True
    except:
        return False


def setup_node(num, port):
    myKeys = []
    myPredecessorKeys = []
    myFingerTable = {}
    node_connections = {}
    connections = []


    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.listen(33)

    if(num == 0):
        for i in range(256): #ONLY FOR FIRST NODE
            myKeys.append(i)
        for j in range(8):
            myFingerTable[j] = 0

    data = {
            'myNum' : num,
            'myKeys': myKeys,
            'myPredecessorKeys': myPredecessorKeys,
            'myFingerTable': myFingerTable,
            'node_connections': node_connections,
            'connections' : connections,
    }



    connectToNodes(num, node_connections) # connect to other nodes

    if(num!=0):
        getKeys(data)


    while True:
        conn, addr = s.accept()
        connections.append(conn)
        conn_thread = Thread(target = readMessages, args = (conn,data))
        conn_thread.start()


def getKeys(data):
    num = data['myNum']
    node_connections = data['node_connections']
    allKeys = sorted(node_connections.keys())
    successor_num = -1
    predecessor_num = -1

    for key in allKeys:
        if(int(key)>num):
            successor_num = int(key)
    if(successor_num == -1):
        successor_num = 0

    if(num == 0):
        predecessor_num = allKeys[-1]

    for key in reversed(allKeys): #find successor
        if(int(key)<num):
            predecessor_num = int(key)

    socket = node_connections[successor_num]

    msg = {
        'source' : "server",
        'myNum' : num,
        'request': "take",
    }

    sendMessage(msg,socket) # send request to take keys from successor

    msg = {
        'source' : "server",
        'myNum' : num,
        'request': "recover",
    }

    second_socket = node_connections[predecessor_num]
    sendMessage(msg, socket)


def sendMessage(msg, socket):
    serialized_message = pickle.dumps(msg, -1)
    socket.sendall(serialized_message)




def connectToNodes(num, node_connections):
    for node in node_sockets:
        if(int(node)!=num):
            node_connections[int(node)] = node_sockets[node][1]

def readMessages(conn,data):
    myNum = data['myNum']
    myKeys = data['myKeys']
    myFingerTable = data['myFingerTable']
    while True:
        dump = conn.recv(1024)
        if (not dump):
            break

        message_obj = pickle.loads(dump)
        if(message_obj['source'] == "client"): # message from client
            message = message_obj['message']



            if(message == "show"):
                print("Node: " + str(myNum))
                print("FingerTable: ")
                for finger in range(len(myFingerTable)):
                    sys.stdout.write(str(myFingerTable[finger]) + ",")
                print("")
                print("Keys: ")
                for key in range(len(myKeys)):
                    sys.stdout.write(str(myKeys[key]) +  ", ")
                print("")
        else: # message from server
            if(message_obj['request'] == "take"): #give certain keys to this guy
                newList = []
                requestNum = message_obj['myNum']
                for i in reversed(range(len(myKeys))):
                    print(str(i) + " ")
                    if(myKeys[i]<=requestNum):
                        newList.append(myKeys[i])
                        myKeys.remove(myKeys[i])



            elif(message_obj['request'] == "recover"): # give all keys to this guy
                print("made it")



'''
Parses the config file for data about min/max delay and port num
'''
def parse_file():
    global port
    counter = 0
    with open('config.txt') as f:
        for line in f:
            process_info = line.split()
            if (counter == 0):
                global min_delay, max_delay
                min_delay = int(process_info[0])
                max_delay = int(process_info[1])
            else:
                port = int(process_info[0]) #FIXXX THISSS
                # port = 2000
            counter += 1

# To run the main function
if __name__ == "__main__":
    if (len(sys.argv) != 1):
        print("python " + sys.argv[0])
    else:
        main()