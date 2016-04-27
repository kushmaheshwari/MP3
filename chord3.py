import sys
import signal
from threading import Thread, Lock
import socket
import pickle
import datetime
import time
import random
import signal
import Queue
import copy

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
        # print("Trying to connect to node 0")
        time.sleep(0.5)
    while True:
        invalid_command = False
        user_input = raw_input('')
        # Check if a valid command
        if (user_input):
            input_split = user_input.split()
            if (len(input_split) > 1):
                isDigit = False;
                if(input_split[1].isdigit()):
                    node_num = int(input_split[1])
                    isDigit = True;

                # Joins a node to the chord
                if(input_split[0] == "join" and isDigit):
                    newport = port + node_num
                    create_node = Thread(target=setup_node, args = (node_num, newport))
                    create_node.daemon = True
                    create_node.start()
                    while(clientToNode(node_num, int(newport)) == False):
                        print("setup_client: Trying to connect to node "  + input_split[1])
                        time.sleep(1)

                # Finds where a key is stored
                elif(len(input_split) > 2 and input_split[0] == "find" and input_split[1].isdigit() and input_split[2].isdigit()):
                    keynum = int(input_split[2])
                    msg = {
                        'source': "client",
                        'action': 'Find key',
                        'keynum': keynum,
                        'message': "fuck me"
                    }
                    socket = client_connections.get(int(input_split[1]))
                    serialized_message = pickle.dumps(msg,-1)
                    socket.sendall(serialized_message)
                    data = socket.recv(1024)
                    response = pickle.loads(data)
                    print("Nodenum: " + str(response['Nodenum']))
                # Clean crashes a node
                elif(input_split[0] == "crash" and isDigit):
                    print("Crashing node " + input_split[1])

                # Shows a node's information
                elif(input_split[0] == "show" and isDigit):
                    # print("Showing node")
                    socket = client_connections.get(node_num)
                    if (socket is None):
                        print(input_split[1] + ' does not exist or has crashed')
                    else:
                        msg = {
                            'source': "client",
                            'message' : user_input,
                        }
                        serialized_message = pickle.dumps(msg, -1)
                        socket.sendall(serialized_message)

                        data = socket.recv(1024)
                        response = pickle.loads(data)

                        clientPrintShow(response)

                # Show all nodes' information
                elif(input_split[0] == "show" and input_split[1] == "all"):
                    print("Showing all nodes info")
                else:
                    invalid_command = True
            else:
                invalid_command = True
        if (invalid_command):
            print("Invalid command. Valid commands are:")
            print("join <p>, find <p> <k>, crash <p>, show <p>, show all")

'''
Prints out what a node returned on show on the client side
'''
def clientPrintShow(msg):
    print('====== Showing Node ' + str(msg['num']) + ' ======')
    print('Finger table: ' + str(msg['myFingerTable']))
    print('Keys: ' + str(msg['myKeys']))
    print('PredecessorKeys: ' + str(msg['myPredecessorKeys']))
    print('Predecessor: ' + str(msg['myPredecessor']))
    print('============================')

'''
Client connecting to every node that is created
'''
def clientToNode(num, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.connect(("127.0.0.1", port))
        client_connections[num] = s
        # print("clientToNode: Connected to node " + str(num))
        return True
    except:
        return False

'''
Thread function for node to receive connections
'''
def accept_connections(node):
    socket = getattr(node, 'socket')

    while True:
        conn, addr = socket.accept()
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

        # print(str(getattr(node, 'num')) + ' received a message')

        message_obj = pickle.loads(dump)
        if(message_obj['source'] == "client"): # message from client
            message = message_obj['message'].split()
            # Show message
            if (message[0] == "show"):
                if (message[1] == "all"):
                    showAll(node, conn)
                else:
                    showMine(node, conn)
            # Find key
            elif (message_obj['action'] == 'Find key'):#node that recieves request to find a key
                findNode(node,message_obj,conn)
            # Crash current node
            elif (message[0] == "crash"):
                continue

        else: # message from node
            # print("readMessages: " + str(getattr(node, 'num')) + " received a message: " + message_obj['action'])
            # Set your predecessor to node
            if (message_obj['action'] == 'Set predecessor'):
                setPredecessor(node, message_obj)
            # Update your finger table
            elif (message_obj['action'] == 'Update finger table'):
                updateFingerTable(node, message_obj['s'], message_obj['i'])
            # Node requesting my info
            elif (message_obj['action'] == 'Requesting node info'):
                sendNodeInfo(node, message_obj['num'], conn)
            # Node is joining the chord system (only node 0)
            elif (message_obj['action'] == 'Joining chord system'):
                # print("readMessages: " + str(getattr(node, 'num')) + " received a join request")
                join(node, message_obj['node'], conn)
            elif(message_obj['action'] == 'Find Key'):
                findNode2(node,message_obj)
            elif(message_obj['action'] == 'Found Node with Key'):#send to client
                msg = {
                    'source': "Node",
                    'action': "Found Key",
                    'node_num': message_obj['Nodenum']
                }
                print("nodenum: " + str(message_obj['Nodenum']))
            elif(message_obj['action'] == 'Take Keys'):
                takeKeys(node,message_obj, conn)
            elif(message_obj['action'] == 'Give Predecessor Keys'):
                updatePredecessorKeys(node,message_obj)

        # print("readMessages: Node " + str(getattr(node, 'num')) + " has handled the request")


def findNode(node,message_obj,conn):#this is for the first node
    keynum = message_obj['keynum']
    myKeys = getattr(node,'myKeys')
    for i in range(len(myKeys)):
        if(myKeys[i]==keynum):#found key in this node
            msg = {
                'source' : "Node",
                'action' : "Found Node with Key",
                'Nodenum': getattr(node,'num') 
            }
            serialized_message = pickle.dumps(msg, -1)
            conn.sendall(serialized_message)#send back to client(need to add clients socket)
            return
    myFingerTable = getattr(node,'myFingerTable')
    queryNode = -1
    for i in range(len(myFingerTable)):#will be a sorted finger table
        if(myFingerTable[i]<=keynum):#might have to do stronger check
            queryNode=myFingerTable[i]#send find to this node
    if(queryNode==-1):
        queryNode==myFingerTable[0]#set to successor
    msg = {
        'source' : "Node",
        'action' : "Find Key",
        'keynum' : message_obj['keynum'],
        'sourcenode': getattr(node,'num') 
    }
    sendNode2NodeMessage(node, msg, queryNode)#sends message out to other node


def findNode2(node,message_obj):#this is for every other node
    keynum = message_obj['keynum']
    myKeys = getattr(node,'myKeys')
    for i in range(len(myKeys)):
        if(myKeys[i]==keynum):#found key in this node
            msg = {
                'source' : "Node",
                'action' : "Found Node with Key",
                'Nodenum': getattr(node,'num') 
            }
            sendNode2NodeMessage(node,msg,message_obj['sourcenode'])#send message back to original noce
            return
    myFingerTable = getattr(node,'myFingerTable')
    queryNode = -1
    for i in range(len(myFingerTable)):#will be a sorted finger table
        if(myFingerTable[i]<=keynum):#might have to do stronger check
            queryNode=myFingerTable[i]#send find to this node
    if(queryNode==-1):
        queryNode==myFingerTable[0]
    msg = {
        'source' : "Node",
        'action' : "Find Key",
        'keynum' : message_obj['keynum'],
        'sourcenode': message_obj['sourcenode'] 
    }
    sendNode2NodeMessage(node,msg,queryNode)#send to query node


'''
Sets the node passed in the message as its predecessor
Do I even really need a function for this shit?
'''
def setPredecessor(node, message):
    setattr(node, 'myPredecessor', message['num'])
    # setattr(node,'myFingerTable',fingers)

'''
Sets the keys of the specified node
'''
def setKeys(node):
    num = getattr(node, 'num')

    keys = []
    if (num == 0): # set all keys 0-255 for node 0
        for i in range(256):
            keys.append(i)
        setattr(node,'myKeys',keys)
        return



''' ====== Implementing show ====== '''

'''
Show all information about the node and send it back to the client
'''
def showMine(node, conn):
    msg = {
        'num': getattr(node, 'num'),
        'myFingerTable': getattr(node, 'myFingerTable'),
        'myKeys': getattr(node, 'myKeys'),
        'myPredecessorKeys': getattr(node, 'myPredecessorKeys'),
        'myPredecessor': getattr(node, 'myPredecessor')
    }

    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)


''' ====== Implementing join ====== '''

'''
Thread function to create the node and begin reading messages
'''
def setup_node(num, port):
    node = Node(num, port) # create a new node

    if (node is None):
        print("Node is null")

    if(num == 0):
        setattr(node,'myPredecessor', 0)

    accept_conn_thread = Thread(target = accept_connections, args = (node,))
    accept_conn_thread.daemon = True
    accept_conn_thread.start()

    # print("Node " + str(num) + " is entering joinChordSystem")

    joinChordSystem(node) # sets its finger table

    # print("Fingers are now: " + str(getattr(node, 'myFingerTable')))

    # print("joinChordSystem: Node " + str(num) + " has completed join")

    if (node is None):
        print("Node is null after joinChordSystem")

    # setKeys(node) # sets its keys

    #joinNodes(node) # Join the node to the nodes in its finger table now


'''
Sets the finger table of the node
Node 0 sets its own finger table
Any other node requests access through node 0
'''
def joinChordSystem(node):
    # print("joinChordSystem: " + str(getattr(node, 'num')))

    # If node 0, that means only node in chord (set fingers to 0)
    num = getattr(node, 'num')
    fingers = []
    keys = []
    if (num == 0):
        # print("THIS IS NODE 0")
        for i in range(8):
            fingers.append(0)
        setattr(node, 'myFingerTable',fingers)
        for i in range(256):
            keys.append(i)
        setattr(node,'myKeys',keys)
        setattr(node,'myPredecessorKeys',keys)
        return

    temp = node

    msg = {
        'source' : "Node",
        'action' : "Joining chord system",
        'node': temp,
        'num' : num
    }

    # print("joinChordSystem: Sending message to node 0 requesting access")
    sendNode2NodeMessage(node, msg, 0) #send message to node 0 that you want to be entered into the system
    # print("joinChordSystem: Sent message to node 0, waiting for response now")

    # Wait for response
    node_connections = getattr(node, 'node_connections')
    data = node_connections[0].recv(1024)
    response = pickle.loads(data)
    response_node = response['node']

    # print("joinChordSystem: Received response from node 0")

    # print("Fingers are : " + str(getattr(response_node, 'myFingerTable')))

    setattr(node, 'myFingerTable', getattr(response_node, 'myFingerTable'))
    setattr(node, 'myPredecessor', getattr(response_node, 'myPredecessor'))

    # print("joinChordSystem: Node " + str(num) + ": Before updating others")
    updateOthers(node)
    moveKeys(node)
    # print("joinChordSystem: Node " + str(num) + ": After updating others")

    # print("joinChordSystem: Node " + str(num) + ":Join complete")
    print("Ack")

    return node

def moveKeys(node):# move keys in (predecessor,node] from successor
    myPredecessor = getattr(node,'myPredecessor')#
    msg = {
        'source': "Node",
        'action': 'Take Keys',
        'nodenum': getattr(node,'num'),
        'predecessor': myPredecessor,
    }
    successor = getattr(node,"myFingerTable")[0]


    sendNode2NodeMessage(node, msg, successor)#sends message to successor node asking to take keys
    node_connections = getattr(node, 'node_connections')
    data = node_connections[successor].recv(1024)
    response = pickle.loads(data)#should get the keys back
    keys = response['keys']
    setattr(node,'myKeys',keys)#set your keys to the incoming keys
    setattr(node, 'myPredecessorKeys', response['predecessorKeys'])


def takeKeys(node,message_obj, conn):
    myKeys = getattr(node,'myKeys')
    backupKeys = []
    predecessor = message_obj['predecessor']
    num = message_obj['nodenum']
    for i in range(len(myKeys)):
        if(myKeys[i]>predecessor and myKeys[i]<=num):
            backupKeys.append(myKeys[i])
            
    for i in reversed(range(len(myKeys))):
        if(myKeys[i]>predecessor and myKeys[i]<=num):
            myKeys.remove(myKeys[i])

    predecessorKeys = getattr(node,'myPredecessorKeys')
 

    setattr(node,'myKeys',myKeys)#set keys to new keys
    setattr(node,'myPredecessorKeys',backupKeys)# these will also be sent back to the node that asked for keys
    successor = getattr(node,'myFingerTable')[0]
    msg = {
        'source': "Node",
        'action': "Give Predecessor Keys",
        'predecessorKeys': getattr(node,'myKeys')
    }
    serialized_message = pickle.dumps(msg, -1)
    sendNode2NodeMessage(node,msg,successor)
    msg = {
        'source': "Node",
        'action': 'Give Keys',
        'keys': backupKeys,
        'predecessorKeys': predecessorKeys
    }

    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)
    # sendNode2NodeMessage(node, msg, int(num))#sends message back to predecessor that asked for keys
    
def updatePredecessorKeys(node,message_obj):
    setattr(node,'myPredecessorKeys',message_obj['predecessorKeys'])


'''
node joins the network
cur_node is an arbitrary node in the network (will be 0)
'''
def join(cur_node, node, conn):
    # print("join: Trying to join " + str(getattr(node, 'num')) + " to chord")

    fingers = getattr(node, 'myFingerTable')
    # print("join: " + str(fingers))

    initializeFingerTable(cur_node, node)

    fingers = getattr(node, 'myFingerTable')
    # print("join: After initializing finger table: " + str(fingers))

    temp = node

    msg = {
        'source': 'Node',
        'action': 'Joined node to chord',
        'num': getattr(cur_node, 'num'),
        'node': temp
    }

    msg = removeSocketsFromMessage(msg)

    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)


    # print("join: Responded to join request")

    # sendNode2NodeMessage(cur_node, msg, getattr(node, 'num'))

'''
Initialize the finger table of node
cur_node is an arbitrary node in the network
'''
def initializeFingerTable(cur_node, node):
    # print("initializeFingerTable: Start")
    nodeFingerTable = getattr(node, 'myFingerTable')
    node_num = getattr(node, 'num')

    # Set finger[0]
    finger_start = (node_num + 2**0) % (2**8)
    # print("initializeFingerTable: Trying to find successor")
    successor = findSuccessor(cur_node, finger_start)
    nodeFingerTable[0] = getattr(successor, 'num')
    # print("initializeFingerTable: Found successor = " + str(nodeFingerTable[0]))

    # predecessor = successor.predecessor
    setattr(node, 'myPredecessor', getattr(successor, 'myPredecessor'))

    # successor.predecessor = n
    setattr(node, 'myFingerTable', nodeFingerTable)
    setSuccessorPredecessor(cur_node, node)

    for i in range(7):
        finger_start = (node_num + 2**(i+1)) % (2**8)
        # print(str(i) + ": " + str(finger_start))
        # if (node_num <= finger_start < nodeFingerTable[i]):
        if (checkInterval(node_num, finger_start, nodeFingerTable[i], True, False)):
            # print("initializeFingerTable: Node " + str(node_num) + ": Setting " + str(i+1) + " to " + str(nodeFingerTable[i]))
            nodeFingerTable[i+1] = nodeFingerTable[i]
        else:
            nodeFingerTable[i+1] = getattr(findSuccessor(cur_node, finger_start), 'num')
            # print("initializeFingerTable: Node " + str(node_num) + ": Setting " + str(i+1) + " to " + str(nodeFingerTable[i+1]))
    setattr(node, 'myFingerTable', nodeFingerTable)
    # print("initializeFingerTable: End")

'''
Sends a message to node's successor to update its predecessor to be node
'''
def setSuccessorPredecessor(cur_node, node):
    # successor = getattr(node, 'mySuccessor')
    successor = getattr(node, 'myFingerTable')[0]
    num = getattr(cur_node, 'num')

    if (num != successor):
        msg = {
            'source': 'Node',
            'action': 'Set predecessor',
            'num': getattr(node, 'num')
        }\

        sendNode2NodeMessage(cur_node, msg, successor)
    else:
        setattr(cur_node, 'myPredecessor', getattr(node, 'num'))
        # print("setSuccessorPredecessor: " + str(getattr(cur_node, 'num')) + "'s predecessor is now " + str(getattr(cur_node, 'myPredecessor')))


''' -- Updating others finger tables -- '''

'''
Updates all nodes whose finger tables should refer to n
'''
def updateOthers(node):
    num = getattr(node, 'num')
    # print("updateOthers: Start")
    for i in range(8):
        # Find the last node whose ith finger might be node
        predecessorValue = (num - (2**i)) % (2**8)
        # print(str(num) + "'s updateOthers: Before finding predecessorNode")
        predecessorNode = findPredecessor(node, predecessorValue)
        # print(str(num) + "'s updateOthers: Found " + str(i) + "th predecessor node: " + str(getattr(predecessorNode, 'num')) )

        # Send a message to the predecessor to update its finger table
        sendUpdateFingerMessage(node, predecessorNode, num, i)

        # updateFingerTable(predecessorNode, node, i)
    # print("updateOthers: End")

'''
Sends a message to node telling it to update its finger table
'''
def sendUpdateFingerMessage(node, predecessorNode, s, i):
    msg = {
        'source': 'Node',
        'action': 'Update finger table',
        's': s,
        'i': i
    }

    # print("sendUpdateFingerMessage: " + str(getattr(predecessorNode, 'num')) + ', ' + str(s) + ', ' + str(i))

    sendNode2NodeMessage(node, msg, getattr(predecessorNode, 'num'))

'''
If node s is the ith finger table entry of node n, update n's finger table with s
'''
def updateFingerTable(node, s, i):
    num = getattr(node, 'num')
    myFingerTable = getattr(node, 'myFingerTable')
    my_finger_temp = myFingerTable[i]
    if (myFingerTable[i] <= num):
        my_finger_temp += 256

    # time.sleep(1)

    # print(str(num) + " received updateFingerTable: " + str(num) + " < " + str(s) + " < " + str(my_finger_temp))
    if ( num < s < my_finger_temp):
        # print(str(num) + " updateFingerTable: Setting " + str(i) + " to " + str(s))
        myFingerTable[i] = s
        setattr(node, 'myFingerTable', myFingerTable)

        # p = findPredecessor(node, num)
        p = requestNodeInfo(node, getattr(node, 'myPredecessor'))
        # print("Found predecessor of " + str(num) + ' = ' + str(getattr(p, 'num')))

        sendUpdateFingerMessage(node, p, s, i)

'''
Returns the information of the successor of value
'''
def findSuccessor(node, value):
    # print("findSuccessor: " + str(value))
    predecessor = findPredecessor(node, value)
    successor = requestNodeInfo(node, getattr(predecessor, 'myFingerTable')[0])
    return successor

'''
Asks a node to find the predecessor of the node with id = value
'''
def findPredecessor(node, value):
    # Get node's information
    predecessorNode = node
    num = getattr(predecessorNode, 'num')
    successor = getattr(predecessorNode, 'myFingerTable')[0]
    # if (successor == 0):    # 0 = 256 in a chord
        # successor = 256

    # print("findPredecessor: Looking for closest preceding finger")
    # print(str(num) + "'s findPredecessor: " + str(num) + " < " + str(value) + " <= " + str(successor))
    # while (not (num < value <= successor)):
    while (not checkInterval(num, value, successor, False, True)):
        # print(str(num) + "'s findPredecessor: " + str(num) + " < " + str(value) + " <= " + str(successor))
        predecessorNode = getClosestPrecedingFinger(node, predecessorNode, value)

        # Check for infinite loop
        if (getattr(predecessorNode, 'num') == num):
            break
        num = getattr(predecessorNode, 'num')
        successor = getattr(predecessorNode, 'myFingerTable')[0]
        # if (successor == 0):
            # successor = 256
        # print(str(num) + "'s findPredecessor: " + str(num) + " < " + str(value) + " <= " + str(successor))
        # time.sleep(0.2)
    # print(str(num) + "'s findPredecessor: Found closest preceding finger with num " + str(num))
    # sys.stdout.flush()
    return predecessorNode


def checkInterval(beg,num,end,beginclusive,endinclusive):
    if(beginclusive and endinclusive):
        if(beg <= end):
            return (beg <= num <= end)
        else:
            return (beg <= num or num <= end)
    elif(beginclusive and (not endinclusive)):
        if(beg <= end):
            return (beg <= num < end)
        else:
            return (beg <= num or num < end)
            # return (num < end and num >= beg)
    elif(endinclusive and (not beginclusive)):
        if(beg <= end):
            return (beg < num <= end)
        else:
            return (beg < num or num <= end)
            # return (num <= end and num >= beg)
    else:
        if(beg <= end):
            return (beg < num < end)
        else:
            return (beg < num or num < end)
            # return (num < end and num > beg)

'''
Returns the closest finger preceding the node of value
'''
def getClosestPrecedingFinger(cur_node, node, value):
    num = getattr(node, 'num')
    # print("num = " + str(num) + ", value = " + str(value))

    myFingerTable = getattr(node, 'myFingerTable')
    # print("getClosestPrecedingFinger: " + str(myFingerTable))
    for i in reversed(range(8)):
        # print(str(num) + "'s getClosestPrecedingFinger: " + str(num) + " < " + str(myFingerTable[i]) + " < " + str(value))
        # if (num < myFingerTable[i] < value):
        if (checkInterval(num, myFingerTable[i], value, False, False)):
            # print(str(num) + "'s getClosestPrecedingFinger: Found " + str(myFingerTable[i]))
            return requestNodeInfo(cur_node, myFingerTable[i])
    # else:
    #     for i in reversed(range(8)):
    #         print(str(num) + "'s getClosestPrecedingFinger: " + str(value) + " < " + str(myFingerTable[i]) + " < " + str(num))
    #         if (not (value <= myFingerTable[i] <= num)):
    #             print(str(num) + "'s getClosestPrecedingFinger: Found " + str(myFingerTable[i]))
    #             return requestNodeInfo(node, myFingerTable[i])

    return node

'''
Sends a message to the node with value to request its information
'''
def requestNodeInfo(node, value):
    myNum = getattr(node, 'num')
    # print(str(myNum) + "'s requestNodeInfo: " + str(myNum) + ' requesting info from ' + str(value))

    if (myNum != value):

        msg = {
            'source': 'Node',
            'action': 'Requesting node info',
            'num': myNum
        }

        # print("requestNodeInfo: Sending message to " + str(value))
        sendNode2NodeMessage(node, msg, value)

        node_connections = getattr(node, 'node_connections')

        data = node_connections[value].recv(1024);
        response = pickle.loads(data)

        # print(str(myNum) + "'s requestNodeInfo: " + str(myNum) + ' received info from ' + str(value))

        return response['node']

    # print(str(myNum) + "'s requestNodeInfo: Requesting from myself!")
    return node

'''
Sends the current node info back to the requesting node
'''
def sendNodeInfo(node, num, conn):
    temp = node
    msg = {
        'source': 'Node',
        'action': 'Responding node information',
        'node': temp
    }

    msg = removeSocketsFromMessage(msg)

    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)

    # sendNode2NodeMessage(node, msg, num)

'''
Remove sockets from node sent in a message
Pickle does not support sending sockets
'''
def removeSocketsFromMessage(msg):
    msg_no_sockets = copy.deepcopy(msg)
    if ('node' in msg_no_sockets):
        if (hasattr(msg_no_sockets['node'], 'node_connections')):
            del msg_no_sockets['node'].node_connections
        if (hasattr(msg_no_sockets['node'], 'socket')):
            del msg_no_sockets['node'].socket
    # setattr(msg['node'], 'node_connections', None)
    # setattr(msg['node'], 'socket', None)
    return msg_no_sockets

'''
Sends a message to another node given the message and the node's id
'''
def sendNode2NodeMessage(node, msg, num):
    #time.sleep((random.uniform(min_delay, max_delay)/1000.0))
    node_connections = getattr(node, 'node_connections')
    if (node_connections is None):
        node_connections = {}
    if (not (num in node_connections)):
        node_connections[num] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_connections[num].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = num + 2000
        try:
            # print('sendNode2NodeMessage: ' + str(getattr(node, 'num')) + ' making connection to ' + str(num))
            # print('sendNode2NodeMessage: connecting at port ' + str(port))
            node_connections[num].connect(("127.0.0.1", port))
            setattr(node, 'node_connections', node_connections)

            if ('node' in msg):
                msg = removeSocketsFromMessage(msg)

        except Exception, e:
            print("sendNode2NodeMessage: " + str(e))

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
        self.myFingerTable = [-1] * 8
        self.node_connections = {} # who I can connect to
        self.connections = [] # who's connected to me
        self.myPredecessor = -1
        self.socket = -1

        self.setup_node(port)

        Node.count += 1

    def create_server(self, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # print("Trying to connect to port " + str(port))
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


'''
Signal handler to gracefull close chord system
'''
def signal_handler(signal, frame):
    print("\nClosing chord system.")
    sys.exit(0)


# To run the main function
if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    if (len(sys.argv) != 1):
        print("python " + sys.argv[0])
    else:
        main()