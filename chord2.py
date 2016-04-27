
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

# Ran with commands “python client.py”
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
        print(“Trying to connect to nodeee”)
    while True:
        user_input = raw_input('')
        # Check if a valid command
        if (user_input):
            input_split = user_input.split()
            if (len(input_split) > 1):
                isDigit = False;
                if(input_split[1].isdigit()):
                    node_num = input_split[1]
                    isDigit = True;

                # Joins a node to the chord
                if(input_split[0] == “join” and isDigit):
                    newport = port + int(input_split[1])
                    create_node = Thread(target=setup_node, args = (node_num,newport))
                    create_node.daemon = True
                    create_node.start()
                    while(clientToNode(node_num, int(newport)) == False):
                        print(“Trying to connect to nodedd “ + input_split[1])

                # Finds where a key is stored
                elif(len(input_split) > 2 and input_split[0] == “find” and isDigit and input_split[2].isdigit()):
                    keynum = int(input_split[2])

                # Clean crashes a node
                elif(input_split[0] == “crash” and isDigit):
                    print(“Crashing node “ + input_split[1])

                # Shows a node's information
                elif(input_split[0] == “show” and input_split[1].isdigit()):
                    print(“Showing node “)
                    socket = node_sockets[int(input_split[1])][1]
                    msg = {
                        'source': “client”,
                        'message' : “show”,
                    }
                    serialized_message = pickle.dumps(msg, -1)
                    socket.sendall(serialized_message)



                # Show all nodes' information
                elif(input_split[0] == “show” and input_split[1] == “all”):
                    print(“Showing all nodes info”)

                else:
                    print(“Invalid Command”)

'''
Client connecting to every node that is created
'''
def clientToNode(num, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.connect((“127.0.0.1”, port))
        node_sockets[num] = (port,s)
        print(“Connected to node “ + str(num))
        return True
    except:
        return False


def setup_node(num, port):


    node = Node(num,port) # create a new node
    if(num == 0):
        setattr(node, 'myPredecessor', 0)
        setattr(node, 'mySuccessor', 0)
    setFingers(node) # sets its finger table
    setKeys(node) # sets its keys



    #joinNodes(node) # Join the node to the nodes in its finger table now


    s = getattr(node,'sock')
    while True:
        conn, addr = s.accept()
        #connections.append(conn)
        conn_thread = Thread(target = readMessages, args = (conn,node))
        conn_thread.start()



def readMessages(conn,node):

    while True:
        dump = conn.recv(1024)
        if (not dump):
            break

        message_obj = pickle.loads(dump)
        if(message_obj['source'] == “client”): # message from client
            message = message_obj['message']



            if(message == “show”):
                print(“Node: “ + str(myNum))
                print(“FingerTable: “)
                for finger in range(len(myFingerTable)):
                    sys.stdout.write(str(myFingerTable[finger]) + “,”)
                print(“”)
                print(“Keys: “)
                for key in range(len(myKeys)):
                    sys.stdout.write(str(myKeys[key]) + “, “)
                print(“”)
        else: # message from node
            if(message_obj['action'] == “Entering chord system”): #New node is joining the system. Send finger table
                num = message_obj['num'] #id of new node
                mySuccessor = getattr(node, 'mySuccessor')#node 0's successor
                myNum = getattr(node, 'num')#num of node receiving this message

                # If node 0 is the only node in the system (i.e. is its own successor)
                if(mySuccessor == 0 or num < mySuccessor): #return back 0 as successor to the node that asked to be entered
                    myPredecessor = getattr(node,'myPredecessor')
                    msg = {
                        'source' : “Node”,
                        'action' : “Giving successor”,
                        'successor' : mySuccessor,
                        'predecessor' : myPredecessor,
                        'num' : myNum,
                    }
                    setattr(node, 'mySuccessor', num)#setting node0 successor to the new node
                    setattr(node, 'myPredecessor', num)#setting node0 successor to the new node
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    try:
                        port = int(num)+2000
                        print(“Making connection”)
                        s.connect((“127.0.0.1”, port))
                        sendNode2NodeMessage(msg,s)
                    except:
                        print(“Fuck my life”)
                else:#new id is not in between 0 and 0s successor
                    myFingerTable= getattr(node,'myFingerTable')
                    placeholder = 0
                    forwardednode = -1
                    for i in reversed(range(8)):
                        if(myFingerTable[i]!=0):
                            if(num > myFingerTable[i]):
                                forwardednode = myFingerTable[i]#this is predecessor of new node being added
                                break;
                    if(forwardednode == -1):#node 0s finger table isnt good enough, forward query to last node in finger table
                        newport=myFingerTable[7]+2000#port number of node that u r forwarding request too
                        msg = {
                            'source' : “Node”,
                            'action' : “ForwardedQuery”,
                            'num' : num,#number of node trying to join system
                            'forwardedNum': myNum, #number of node that sent forwarded query
                        }
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        try:
                            s.connect((“127.0.0.1”, newport))
                            sendNode2NodeMessage(msg,s)
                        except:
                            print(“Fuck my life”)

                    else:#forwardednode's successor is now the new successor of the entering node
                        newport=forwardednode+2000#port of node
                        msg = {
                            'source' : “Node”,
                            'action' : “PredecessorNewNode”,#predecessor of entering node because ur successor is the successor of enterring node
                            'num' : num,#number of node trying to join system
                            'forwardedNum': myNum, #number of node that sent forwarded query
                        }
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        try:
                            s.connect((“127.0.0.1”, newport))
                            sendNode2NodeMessage(msg,s)
                        except:
                            print(“Fuck my life”)


            elif(message_obj['action'] == “ForwardedQuery”):#query people in my finger table
                myFingerTable = getattr(node,'myFingerTable')
                num = message_obj['num']
                forwardednode = -1
                for i in reversed(range(8)):
                    if(num<myFingerTable[i]): #might have to do another 0 check
                        forwardednode = myFingerTable[i]

                newport=forwardednode+2000#port of node
                msg = {
                    'source' : “Node”,
                    'action' : “PredecessorNewNode”,#predecessor of entering node because ur successor is the successor of enterring node
                    'num' : num,#number of node trying to join system
                    'forwardedNum': myNum, #number of node that sent forwarded query
                }
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.connect((“127.0.0.1”, newport))
                    sendNode2NodeMessage(msg,s)
                except:
                    print(“Fuck my life”)

            elif(message_obj['action'] == “PredecessorNewNode”):
                num = message_obj['num'] #id of new node
                mySuccessor = getattr(node, 'mySuccessor')#my successor which is now new nodes successor
                myNum = getattr(node, 'num')#num of node receiving this message
                msg = {
                    'source' : “Node”,
                    'action' : “Giving successor”,
                    'successor' : mySuccessor,#successor of new node
                    'predecessor' : myNum,#you are predecessor of new ndoe
                }
                setattr(node,'mySuccessor',num)#setting my successor to be the new node entering scheme(SOMEHOW HAve to tell current successor that his predecessor is new node)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                newport = num+2000
                try:
                    s.connect((“127.0.0.1”, newport))
                    sendNode2NodeMessage(msg,s)
                except:
                    print(“Fuck my life”)

'''
Updates all nodes whose finger tables should refer to n
'''
def updateOthers(node):
    for i in range(8):
        num = getattr(node, 'num')
        # Find the last node whose ith finger might be in n
        nodeValue = num - (2**i)
        predecessorNode = findPredecessor(node, nodeValue)

'''
Finds predecessor of the node of value
'''
def findPredecessor(node, value):
    num = getattr(node, 'num')
    myFingerTable = getattr(node, 'myFingerTable')
    while (not (num <= value <= myFingerTable[0])):
        node = getClosestPrecedingFinger(node, value)

'''
Returns the closest finger preceding the node of value
'''
def getClosestPrecedingFinger(node, value):
    num = getattr(node, 'num')
    myFingerTable = getattr(node, 'myFingerTable')
    for i in reverse(range(8)):
        if (num <= myFingerTable[i] <= value):
            return requestNodeInfo(node, myFingerTable[i])
    return node

'''
Sends a message to the node with value to request its information
'''
def requestNodeInfo(node, value):
    myNum = getattr(node, 'num')
    msg = {
        'source': 'Node',
        'action': 'Requesting info',
        'num': myNum
    }

    socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        port = value + 2000
        print('Requesting information from ' + str(value))
        socket.connect((“127.0.0.1”, port))
        sendNode2NodeMessage(msg, socket)
    except:
        print('Fuck my life')

    serialized_message = pickle.dumps(msg, -1)
    socket.sendall(serialized_message)

    response = socket.recv(1024);
    response = pickle.loads(data)

    return response['node']

def waitForResponse(conn, node):


def setFingers(node):

    # If node 0, that means only node in chord(set fingers to 0)
    num = getattr(node, 'num')
    fingers = []
    if (num == 0):
        for i in range(8):
            fingers.append(0)
        setattr(node,'myFingerTable',fingers)
        return

    msg = {
        'source' : “Node”,
        'action' : “Entering chord system”,
        'num' : num,
    }
    socket = getattr(node, 'node0socket')
    sendNode2NodeMessage(msg,socket) #send message to node 0 that you want to be entered into the system
    #HOW DO I WAIT FOR RESPONSE(Thread join)
    counter = 0
    s = getattr(node,'sock')
    while (counter == 0):#DO i need while loop?
        conn, addr = s.accept()
        print(“Got connection”)
        conn_thread = Thread(target = waitForSuccessor, args = (conn,node))
        conn_thread.start()
        counter = 1
    print(“waiting to join”)
    conn_thread.join()#Wait for other thread: join right here: now the successor and predecessor of the node should be populated
    successor = getattr(node,'mySuccessor')
    fingers[0] = successor
    if(fingers[0] == 0):#run algorithm
        placeholder = 256
    for i in range(7):
        value = (num + 2**(i+1)) % (2**8)
        if(value<placeholder):
            fingers[i+1]=fingers[i]
        else:
            fingers[i+1]=1000 #find successor of the number (value)

    print(“holy fuck”)
    setattr(node,'myFingerTable',fingers)


def waitForSuccessor(conn, node):
    dump = conn.recv(1024)
    print(“Got dump”)
    message_obj = pickle.loads(dump)
    successor = message_obj['successor']
    predecessor = message_obj['predecessor']
    setattr(node,'mySuccessor',successor)
    setattr(node,'myPredecessor',predecessor)


def setKeys(node):
    num = getattr(node, 'num')

    keys = []
    if (num == 0): # set all keys 0-255 for node 0
        for i in range(256):
            keys.append(i)
        setattr(node,'myKeys',keys)
        return



'''
Defining a class Node a single node in the chord system
'''
class Node:
    count = 0

    def __init__(self, num, port):
        self.num = num
        self.myKeys = []
        self.myPredecessorKeys = []
        self.myFingerTable = []
        self.node_connections = {} # who I can connect to
        self.connections = [] # who's connected to me
        self.myPredecessor = -1
        self.mySuccessor = -1


        self.setup_node(port)
        self.connectNode0(num)


        Node.count += 1

    def create_server(self, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((“127.0.0.1”, port))
        s.listen(32) # Max of 31 other nodes + client can connect
        return s

    def setup_node(self, port):
        self.sock = self.create_server(port)

    def connectNode0(self, num):
        if(num != 0):
            print(“Once”)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.connect((“127.0.0.1”, 2000))
                self.node0socket = s
            except:
                print(“couldnt connect to node 0”)





def sendNode2NodeMessage(msg, socket):
    #time.sleep((random.uniform(min_delay, max_delay)/1000.0))
    serialized_message = pickle.dumps(msg, -1)
    socket.sendall(serialized_message)


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
                port = 2000
            counter += 1



'''
def joinNodes(node):
    num = getattr(node, 'num')
    if(num ==0):
        return
    # Check if only node in chord system
    if (len(node_sockets) == 1):


    for node in node_sockets:
        if(node != num):
            node_connections[node] = node_sockets[node][1]


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
        'source' : “server”,
        'myNum' : num,
        'request': “take”,
    }

    sendMessage(msg,socket) # send request to take keys from successor

    msg = {
        'source' : “server”,
        'myNum' : num,
        'request': “recover”,
    }

    second_socket = node_connections[predecessor_num]
    sendMessage(msg, socket)


'''

# To run the main function
if __name__ == “__main__”:
    if (len(sys.argv) != 1):
        print(“python “ + sys.argv[0])
    else:
        main()