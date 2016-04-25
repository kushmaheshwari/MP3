

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
client_connections = {}

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
                    node_num = input_split[1]
                    isDigit = True;

                # Joins a node to the chord
                if(input_split[0] == "join" and isDigit):
                    newport = port + int(input_split[1])
                    create_node = Thread(target=setup_node, args = (node_num,newport))
                    create_node.daemon = True
                    create_node.start()
                    while(clientToNode(node_num, int(newport)) == False):
                        print("Trying to connect to node "  + input_split[1])

                # Finds where a key is stored
                elif(len(input_split) > 2 and input_split[0] == "find" and isDigit and input_split[2].isdigit()):
                    keynum = int(input_split[2])

                # Clean crashes a node
                elif(input_split[0] == "crash" and isDigit):
                    print("Crashing node " + input_split[1])

                # Shows a node's information
                elif(input_split[0] == "show" and input_split[1].isdigit()):
                    print("Showing node")
                    socket = client_connections[node_num]
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
        client_connections[num] = s
        print("Connected to node " + str(num))
        return True
    except:
        return False

'''
Thread function to create the node and begin reading messages
'''
def setup_node(num, port):
    node = Node(num,port) # create a new node
    if(num==0):
        setattr(node,'myPredecessor',0)
    setFingers(node) # sets its finger table
    setKeys(node) # sets its keys


    #joinNodes(node) # Join the node to the nodes in its finger table now


    s = getattr(node,'socket')
    while True:
        conn, addr = s.accept()
        #connections.append(conn)
        conn_thread = Thread(target = readMessages, args = (conn,node))
        conn_thread.start()

'''
Server of the node that reads messages sent to the node
Depending on the action of the message, executes commands
'''
def readMessages(conn,node):

    while True:
        dump = conn.recv(1024)
        if (not dump):
            break

        message_obj = pickle.loads(dump)
        if(message_obj['source'] == "client"): # message from client
            message = message_obj['message']
        else: # message from node
            if (message_obj['action'] == 'Set predecessor'):
                setPredecessor(node, message_obj)
            elif (message_obj['action'] == 'Update finger table'):
                updateFingerTable(node, message_obj['s'], message_obj['i'])
            elif (message_obj['action'] == 'Requesting node info'):
                sendNodeInfo(node, message_obj['num'], conn)

            # if(message_obj['action'] == "Entering chord system"): #This is for Node 0 receiving.New node is joining the system.
            #     num = message_obj['num'] #id of new node entering
            #     mySuccessor = getattr(node, 'myFingerTable')[0]
            #     # mySuccessor = getattr(node, 'mySuccessor')#node 0's successor
            #     myNum = getattr(node, 'num')#the number 0

            #     if(mySuccessor == 0):#0 is 0s successor. return back 0 as successor to the node that asked to be entered
            #         msg = {
            #             'source' : "Node",
            #             'action' : "Giving successor",
            #             'successor' : mySuccessor,#0
            #             'predecessor' : myNum,#0
            #         }
            #         # setattr(node,'mySuccessor', num)#setting node0 successor to the new node
            #         setattr(node,'myPredecessor',num)#setting node0 predecessor to the new node
            #         sendNode2NodeMessage(msg,int(num))#send message to new node with its info
            #     elif(num < mySuccessor):
            #         msg = {
            #             'source' : "Node",
            #             'action' : "Giving successor",
            #             'successor' : mySuccessor,
            #             'predecessor' : myNum,
            #         }
            #         # setattr(node,'mySuccessor', num)#setting node0 successor to the new node
            #         sendNode2NodeMessage(msg,int(num))#send message to new node with its info
            #         #somehow have to set mySuccessors predecessor to be the new node(send a message)

            #     else:#new id is not in between 0 and 0s successor
            #         myFingerTable= getattr(node,'myFingerTable')#0s finger table
            #         placeholder = 0
            #         forwardednode = -1
            #         for i in reversed(range(8)):
            #             if(myFingerTable[i]!=0):
            #                 if(myFingerTable[i]<num):#CHECK THIS
            #                     forwardednode = myFingerTable[i]#this is predecessor of new node being added
            #                     break;
            #         if(forwardednode==-1):#node 0s finger table isnt good enough, forward query to last node in finger table
            #             newport=myFingerTable[7]#port number of node that u r forwarding request too
            #             msg = {
            #                 'source' : "Node",
            #                 'action' : "ForwardedQuery",
            #                 'num' : num,#number of node trying to join system
            #                 'forwardedNum': myNum, #number of node that sent forwarded query
            #             }
            #             sendNode2NodeMessage(msg,newport)

            #         else:#forwardednode's successor is now the new successor of the entering node
            #             newport=forwardednode#port of node
            #             msg = {
            #                 'source' : "Node",
            #                 'action' : "PredecessorNewNode",#predecessor of entering node because ur successor is the successor of enterring node
            #                 'num' : num,#number of node trying to join system
            #                 'forwardedNum': myNum, #number of node that sent forwarded query
            #             }
            #             sendNode2NodeMessage(msg,newport)


            # elif(message_obj['action'] == "ForwardedQuery"):#query people in my finger table
            #     myFingerTable = getattr(node,'myFingerTable')
            #     num = message_obj['num']
            #     forwardednode = -1
            #     for i in reversed(range(8)):
            #         if(num < myFingerTable[i]): #might have to do another 0 check
            #             forwardednode = myFingerTable[i]

            #     newport=forwardednode#port of node
            #     msg = {
            #         'source' : "Node",
            #         'action' : "PredecessorNewNode",#predecessor of entering node because ur successor is the successor of enterring node
            #         'num' : num,#number of node trying to join system
            #         'forwardedNum': myNum, #number of node that sent forwarded query
            #     }
            #     sendNode2NodeMessage(msg,newport)

            # elif(message_obj['action'] == "PredecessorNewNode"):
            #     num = message_obj['num'] #id of new node
            #     # mySuccessor = getattr(node, 'mySuccessor')#my successor which is now new nodes successor
            #     mySuccessor = getattr(node, 'myFingerTable')[0]
            #     myNum = getattr(node, 'num')#num of node receiving this message
            #     msg = {
            #         'source' : "Node",
            #         'action' : "Giving successor",
            #         'successor' : mySuccessor,#successor of new node
            #         'predecessor' : myNum,#you are predecessor of new ndoe
            #     }
            #     # setattr(node,'mySuccessor',num)#setting my successor to be the new node entering scheme(SOMEHOW HAve to tell current successor that his predecessor is new node)
            #     newport = num + 2000
            #     sendNode2NodeMessage(msg,newport)

'''
Sets the node passed in the message as its predecessor
Do I even really need a function for this shit?
'''
def setPredecessor(node, message):
    setattr(node, 'myPredecessor', message['num'])

'''
Sets the fingers of the current node
'''
def setFingers(node):
    # If node 0, that means only node in chord (set fingers to 0)
    num = getattr(node, 'num')
    fingers = []
    if (num == 0):
        for i in range(8):
            fingers.append(0)
        setattr(node,'myFingerTable',fingers)
        return

    msg = {
        'source' : "Node",
        'action' : "Entering chord system",
        'num' : num,
    }
    sendNode2NodeMessage(node, msg, port) #send message to node 0 that you want to be entered into the system


    #HOW DO I WAIT FOR RESPONSE(Thread join)

    s = getattr(node,'socket')

    conn, addr = s.accept()
    conn_thread = Thread(target = waitForSuccessor, args = (conn,node))
    conn_thread.start()
    conn_thread.join()#Wait for other thread: join right here: now the successor and predecessor of the node should be populated
    # successor = getattr(node,'mySuccessor')

    successor = getattr(node, 'myFingerTable')[0]
    fingers[0] = successor
    if(fingers[0] == 0):#run algorithm
        placeholder = 256
    for i in range(7):
        value = (num + 2**(i+1)) % (2**8)
        if(value<placeholder):
            fingers[i+1]=fingers[i]
        else:
            fingers[i+1]=1000 #find successor of the number (value)

    print("holy fuck")
    setattr(node,'myFingerTable',fingers)


def waitForSuccessor(conn, node):
    dump = conn.recv(1024)
    print("Got dump")
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





''' ====== Implementing join ====== '''




'''
node joins the network
cur_node is an arbitrary node in the network (will be 0)
'''
def join(cur_node, node):
    initializeFingerTable(cur_node, node)
    updateOthers(node)

    msg = {
        'source': 'Node',
        'action': 'Joined node to chord',
        'num': getattr(cur_node, 'num')
    }

    sendNode2NodeMessage(cur_node, msg, getattr(node, 'num'))

'''
Initialize the finger table of node
cur_node is an arbitrary node in the network
'''
def initializeFingerTable(cur_node, node):
    nodeFingerTable = getattr(node, 'myFingerTable')
    node_num = getattr(node, 'num')

    # Set finger[0]
    finger_start = (node_num + 2**0) % (2**8)
    successor = findSuccessor(cur_node, finger_start)
    nodeFingerTable[0] = getattr(successor, 'num')

    # predecessor = successor.predecessor
    setattr(node, 'myPredecessor', getattr(successor, 'myPredecessor'))

    # successor.predecessor = n
    setattr(node, 'myFingerTable', nodeFingerTable)
    setSuccessorPredecessor(node)

    for i in range(7):
        finger_start = (node_num + 2**(i+1)) % (2**8)
        if (node_num <= finger_start <= nodeFingerTable[i]):
            nodeFingerTable[i+1] = nodeFingerTable[i]
        else:
            nodeFingerTable[i+1] = getattr(findSuccessor(cur_node, finger_start), 'num')
    setattr(node, 'myFingerTable', nodeFingerTable)

'''
Sends a message to node's successor to update its predecessor to be node
'''
def setSuccessorPredecessor(node):
    # successor = getattr(node, 'mySuccessor')
    successor = getattr(node, 'myFingerTable')[0]

    msg = {
        'source': 'Node',
        'action': 'Set predecessor',
        'num': getattr(node, 'num')
    }

    sendNode2NodeMessage(node, msg, successor)



''' -- Updating others finger tables -- '''

'''
Updates all nodes whose finger tables should refer to n
'''
def updateOthers(node):
    for i in range(8):
        num = getattr(node, 'num')
        # Find the last node whose ith finger might be node
        predecessorValue = num - (2**i)
        predecessorNode = findPredecessor(node, predecessorValue)

        # Send a message to the predecessor to update its finger table
        sendUpdateFingerMessage(predecessorNode, num, i)

        # updateFingerTable(predecessorNode, node, i)

'''
Sends a message to node telling it to update its finger table
'''
def sendUpdateFingerMessage(node, s, i):
    msg = {
        'source': 'Node',
        'action': 'Update finger table',
        's': s,
        'i': i
    }

    sendNode2NodeMessage(node, msg, getattr(node, 'num'))

'''
If node s is the ith finger table entry of node n, update n's finger table with s
'''
def updateFingerTable(node, s, i):
    num = getattr(node, 'num')
    myFingerTable = getattr(node, 'myFingerTable')
    if (num <= s <= myFingerTable[i]):
        myFingerTable[i] = s

        p = findPredecessor(node, num)
        print("Found predecessor of " + str(num) + ' = ' + getattr(p, 'num'))

        sendUpdateFingerMessage(p, s, i)

'''
Returns the information of the successor of value
'''
def findSuccessor(node, value):
    predecessor = findPredecessor(node, value)
    successor = requestNodeInfo(predecessor, getattr(predecessor, 'mySuccessor'))
    return successor

'''
Asks a node to find the predecessor of the node with id = value
'''
def findPredecessor(node, value):
    predecessorNode = node
    num = getattr(predecessorNode, 'num')
    successor = getattr(predecessorNode, 'myFingerTable')[0]
    while (not (num <= value <= successor)):
        predecessorNode = getClosestPrecedingFinger(predecessorNode, value)
        num = getattr(predecessorsorNode, 'num')
        successor = getattr(predecessorNode, 'myFingerTable')[0]
    return predecessorNode

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
        'action': 'Requesting node info',
        'num': myNum
    }

    sendNode2NodeMessage(node, msg, value)

    node_connections = getattr(node, 'node_connections')

    response = node_connections[value].recv(1024);
    response = pickle.loads(data)

    return response['node']

'''
Sends the current node info back to the requesting node
'''
def sendNodeInfo(node, num, conn):
    msg = {
        'source': 'Node',
        'action': 'Responding node information',
        'node': node
    }

    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)

    # sendNode2NodeMessage(node, msg, num)

'''
Sends a message to another node given the message and the node's id
'''
def sendNode2NodeMessage(node, msg, num):
    #time.sleep((random.uniform(min_delay, max_delay)/1000.0))
    node_connections = getattr(node, 'node_connections')
    if (not num in node_connections):
        node_connections[num] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_connections[num].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = num + 2000
        try:
            print('Making connection to ' + str(num))
            node_connections[num].connect(("127.0.0.1", port))
        except:
            print("Fuck my life")

    serialized_message = pickle.dumps(msg, -1)
    node_connections[num].sendall(serialized_message)

    setattr(node, 'node_connections', node_connections)


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

        self.setup_node(port)

        Node.count += 1

    def create_server(self, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", port))
        s.listen(32) # Max of 31 other nodes + client can connect
        return s

    def setup_node(self, port):
        self.socket = self.create_server(port)







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
