

import sys
import signal
from threading import Thread, Lock
import socket
import pickle
import datetime
import time
import random
import signal
import Queue as Q
import copy

# Dictionary of port and socket for each node
client_connections = {}

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
                if(input_split[0] == “join” and isDigit):
                    if (node_num in client_connections):
                        print(“Node “ + input_split[1] + “ already exists.”)
                    elif (node_num > 255):
                        print('Invalid node ID. Node ID must be [0, 255].')
                    else:
                        newport = port + node_num
                        create_node = Thread(target=setup_node, args = (node_num, newport))
                        create_node.daemon = True
                        create_node.start()
                        while(clientToNode(node_num, int(newport)) == False):
                            print(“setup_client: Trying to connect to node “ + input_split[1])
                            time.sleep(1)

                # Finds where a key is stored
                elif(len(input_split) > 2 and input_split[0] == “find” and isDigit and input_split[2].isdigit()):
                    keynum = int(input_split[2])
                    msg = {
                        'source': “client”,
                        'action': 'Find key',
                        'keynum': keynum,
                        'message': “fuck me”
                    }
                    socket = client_connections.get(node_num)
                    serialized_message = pickle.dumps(msg,-1)
                    socket.sendall(serialized_message)

                    # print(“Client waiting for response”)
                    data = socket.recv(4096)
                    # print(“Client received response”)
                    response = pickle.loads(data)

                    if (response['node_num'] != -1):
                        print(“Key “ + str(keynum) + “ found at Node “ + str(response['node_num']))
                    else:
                        print(“Key “ + str(keynum) + “ not found.”)
                # Clean crashes a node
                elif(input_split[0] == “crash” and isDigit):
                    print(“Crashing node “ + input_split[1])

                # Shows a node's information
                elif(input_split[0] == “show” and isDigit):
                    # print(“Showing node”)
                    socket = client_connections.get(node_num)
                    if (socket is None):
                        print(input_split[1] + ' does not exist or has crashed')
                    else:
                        msg = {
                            'source': “client”,
                            'message' : user_input,
                        }
                        serialized_message = pickle.dumps(msg, -1)
                        socket.sendall(serialized_message)

                        data = socket.recv(4096)
                        response = pickle.loads(data)

                        clientPrintShow(response)

                # Show all nodes' information
                elif(input_split[0] == “show” and input_split[1] == “all”):
                    q = Q.PriorityQueue()

                    # Send and receive show request to all nodes
                    msg = {
                        'source': “client”,
                        'message': user_input
                    }
                    for socket in client_connections:
                        serialized_message = pickle.dumps(msg, -1)
                        client_connections[socket].sendall(serialized_message)

                        data = client_connections[socket].recv(4096)
                        response = pickle.loads(data)

                        q.put( (response['num'], response) )

                    # Once received all shows, print out in order of priority queue
                    while (not q.empty()):
                        clientPrintShow(q.get()[1])

                    # print(“Showing all nodes info”)
                else:
                    invalid_command = True
            else:
                invalid_command = True
        if (invalid_command):
            print(“Invalid command. Valid commands are:”)
            print(“join <p>, find <p> <k>, crash <p>, show <p>, show all”)

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
        s.connect((“127.0.0.1”, port))
        client_connections[num] = s
        # print(“clientToNode: Connected to node “ + str(num))
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
        dump = conn.recv(4096)
        if (not dump):
            break

        message_obj = pickle.loads(dump)

        print('readMessages: Node ' + str(getattr(node, 'num')) + ': ' + str(message_obj))

        if(message_obj['source'] == “client”): # message from client
            if (getattr(node, 'client_socket') == -1):
                setattr(node, 'client_socket', conn)

            message = message_obj['message'].split()
            # Show message
            if (message[0] == “show”):
                showMine(node, conn)
            # Find key
            elif (message_obj['action'] == 'Find key'):#node that recieves request to find a key
                findNodeRequest(node,message_obj,conn)
            # Crash current node
            elif (message[0] == “crash”):
                continue

        else: # message from node
            # print(“readMessages: “ + str(getattr(node, 'num')) + “ received a message: “ + message_obj['action'])
            # Set your predecessor to node
            if (message_obj['action'] == 'Set predecessor'):
                setattr(node, 'myPredecessor', message_obj['num'])
            # Update your finger table
            elif (message_obj['action'] == 'Update finger table'):
                updateFingerTable(node, message_obj['s'], message_obj['i'])
            # Node requesting my info
            elif (message_obj['action'] == 'Requesting node info'):
                sendNodeInfo(node, message_obj['num'], conn)
            # Node is joining the chord system (only node 0)
            elif (message_obj['action'] == 'Joining chord system'):
                # print(“readMessages: “ + str(getattr(node, 'num')) + “ received a join request”)
                join(node, message_obj['node'], conn)
            elif(message_obj['action'] == 'Find Key'):
                findNode2(node,message_obj)
            elif(message_obj['action'] == 'Found Node with Key'):#send to client
                msg = {
                    'source': “Node”,
                    'action': “Found Key”,
                    'keynum': message_obj['keynum'],
                    'node_num': message_obj['node_num']
                }

                # Send information back to client
                serialized_message = pickle.dumps(msg, -1)
                client_socket = getattr(node, 'client_socket')
                client_socket.sendall(serialized_message)
            elif(message_obj['action'] == 'Take Keys'):
                takeKeys(node,message_obj, conn)
            elif(message_obj['action'] == 'Give Predecessor Keys'):
                setattr(node, 'myPredecessorKeys', message_obj['predecessorKeys'])
            elif(message_obj['action'] == 'Heartbeat'):
                receiveHeartbeat(node,message_obj, conn)
            elif(message_obj['action'] == 'superSuccessor' or message_obj['action'] == 'Get successor'):
                sendMySuccessor(node,conn, message_obj)
            elif(message_obj['action'] == 'receiveFingerTableUpdate'):
                receiveFingerTableUpdate(node,message_obj)

        # print(“readMessages: Node “ + str(getattr(node, 'num')) + “ has handled the request”)

'''
Node receives a find node request from the client
'''
def findNodeRequest(node,message_obj,conn): #this is for the first node
    keynum = message_obj['keynum']
    myKeys = getattr(node,'myKeys')

    found = False

    for i in range(len(myKeys)):
        if(myKeys[i] == keynum): #found key in this node
            print(“Found key at current node!”)
            msg = {
                'source' : “Node”,
                'action' : “Found Node with Key”,
                'keynum': keynum,
                'node_num': getattr(node,'num')
            }
            serialized_message = pickle.dumps(msg, -1)
            conn.sendall(serialized_message)#send back to client(need to add clients socket)
            found = True
            break

    # Optimization to check predecessor's keys for keynum
    if (not found):
        predKeys = getattr(node, 'myPredecessorKeys')
        for i in range(len(predKeys)):
            if (predKeys[i] == keynum):
                print(“Found key at predecessor node!”)
                msg = {
                    'source' : “Node”,
                    'action' : “Found Node with Key”,
                    'keynum': keynum,
                    'node_num': getattr(node, 'myPredecessor')
                }
                serialized_message = pickle.dumps(msg, -1)
                conn.sendall(serialized_message)#send back to client(need to add clients socket)
                found = True
                break

    # Else ask other nodes
    if (not found):
        # forwardFindRequest(node, message_obj)

        myFingerTable = getattr(node, 'myFingerTable')
        queryNode = 999
        for i in range(len(myFingerTable)):#will be a sorted finger table
            if (keynum <= myFingerTable[i] and myFingerTable[i] < queryNode):#might have to do stronger check
                print(str(keynum) + “ is less than “ + str(myFingerTable[i]))
                queryNode = myFingerTable[i]#send find to this node
            else:
                break
        if(queryNode == 999):
            queryNode = myFingerTable[0]#set to successor

        msg = {
            'source' : “Node”,
            'action' : “Find Key”,
            'keynum' : message_obj['keynum'],
            'sourcenode': getattr(node,'num'),
            'nodes_visited': 1
        }

        sendNode2NodeMessage(node, msg, queryNode)#sends message out to other node
        print(“Forwarding find to “ + str(queryNode))


def findNode2(node,message_obj):#this is for every other node
    keynum = message_obj['keynum']

    found = False

    # Check if already visited multiple nodes (i.e. key does not exist)
    if (message_obj['nodes_visited'] > 20):
        msg = {
            'source': 'Node',
            'action': “Found Node with Key”,
            'keynum': keynum,
            'node_num': -1
        }
        sendNode2NodeMessage(node, msg, message_obj['sourcenode'])
    else:
        myKeys = getattr(node,'myKeys')
        for i in range(len(myKeys)):
            if(myKeys[i]==keynum):#found key in this node
                msg = {
                    'source' : “Node”,
                    'action' : “Found Node with Key”,
                    'keynum': keynum,
                    'node_num': getattr(node,'num')
                }
                sendNode2NodeMessage(node,msg,message_obj['sourcenode'])#send message back to original noce
                found = True
                break

        if (not found):
            # forwardFindRequest(node, message_obj)

            myFingerTable = getattr(node,'myFingerTable')
            queryNode = 999
            for i in range(len(myFingerTable)):#will be a sorted finger table
                if (keynum <= myFingerTable[i] and myFingerTable[i] < queryNode):#might have to do stronger check
                    print(str(keynum) + “ is less than “ + str(myFingerTable[i]))
                    queryNode = myFingerTable[i]#send find to this node
                else:
                    break
            if(queryNode == 999):
                queryNode = myFingerTable[0]#set to successor

            msg = {
                'source' : “Node”,
                'action' : “Find Key”,
                'keynum' : message_obj['keynum'],
                'sourcenode': message_obj['sourcenode'],
                'nodes_visited': message_obj['nodes_visited'] + 1
            }
            sendNode2NodeMessage(node,msg,queryNode)#send to query node
            print(“Forwarding find to “ + str(queryNode))


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
        print(“Node is null”)

    if(num == 0):
        setattr(node,'myPredecessor', 0)

    accept_conn_thread = Thread(target = accept_connections, args = (node,))
    accept_conn_thread.daemon = True
    accept_conn_thread.start()

    joinChordSystem(node) # sets its finger table

    if (node is None):
        print(“Node is null after joinChordSystem”)


'''
Sets the finger table of the node
Node 0 sets its own finger table
Any other node requests access through node 0
'''
def joinChordSystem(node):
    # print(“joinChordSystem: “ + str(getattr(node, 'num')))

    # If node 0, that means only node in chord (set fingers to 0)
    num = getattr(node, 'num')
    fingers = []
    keys = []
    if (num == 0):
        # print(“THIS IS NODE 0”)
        for i in range(8):
            fingers.append(0)
        setattr(node, 'myFingerTable',fingers)
        for i in range(256):
            keys.append(i)
        setattr(node,'myKeys',keys)
        setattr(node,'myPredecessorKeys',keys)
        setattr(node, 'superSuccessor', 0)
        # crash_thread = Thread(target = sendHeartbeats, args = (node,))#sending out heartbeats to successor
        # crash_thread.daemon = True
        # crash_thread.start()
        return

    msg = {
        'source' : “Node”,
        'action' : “Joining chord system”,
        'node': node,
        'num' : num
    }

    # print(“joinChordSystem: Sending message to node 0 requesting access”)
    sendNode2NodeMessage(node, msg, 0) #send message to node 0 that you want to be entered into the system
    # print(“joinChordSystem: Sent message to node 0, waiting for response now”)

    # Wait for response
    node_connections = getattr(node, 'node_connections')
    data = node_connections[0].recv(4096)
    response = pickle.loads(data)

    print('joinChordSystem (' + str(num) + '): After response from 0: ' + str(response))

    response_node = response['node']

    # print(“joinChordSystem: Received response from node 0”)

    # print(“Fingers are : “ + str(getattr(response_node, 'myFingerTable')))

    setattr(node, 'myFingerTable', getattr(response_node, 'myFingerTable'))
    setattr(node, 'myPredecessor', getattr(response_node, 'myPredecessor'))

    print(“joinChordSystem: Node “ + str(num) + “: Before updating others”)
    updateOthers(node)

    print(“joinChordSystem: Node “ + str(num) + “: After updating others”)

    time.sleep(1)

    moveKeys(node)

    # Getting super successor
    successor = getattr(node,'myFingerTable')[0]
    msg = {
        'source' : “Node”,
        'action' : “superSuccessor”,
    }
    sendNode2NodeMessage(node, msg, successor)
    data = node_connections[successor].recv(4096)
    response2 = pickle.loads(data)

    print('joinChordSystem (' + str(num) + '): After superSuccessor request: ' + str(response2))

    setattr(node,'superSuccessor', response2['successor'])

    print('Fingers: ' + str(getattr(node, 'myFingerTable')))
    print('Predecessor: ' + str(getattr(node, 'myPredecessor')))


    # crash_thread = Thread(target = sendHeartbeats, args = (node,))#sending out heartbeats to successor
    # crash_thread.daemon = True
    # crash_thread.start()
    print(“Ack”)

    return node

''' ========= Implementing Crash =========== '''

'''
'''
def sendHeartbeats(node):
    while True:
        num = getattr(node,'num')
        print(“NUM: “ + str(num))
        successor = getattr(node,'myFingerTable')[0]
        print(“Node “ + str(num) + “ successor is “ + str(successor))
        while(successor==num):#while loop until successor is not yourself. for node 0
            time.sleep(2)
            print(“again”)
            successor = getattr(node,'myFingerTable')[0]
        msg = {
            'source': “Node”,
            'sourcenum': num,
            'action': 'Heartbeat'
        }
        print(“Node “ + str(num) + “ sending heartbeat “ + str(successor))
        sendNode2NodeMessage(node,msg,successor)
        node_connections = getattr(node, 'node_connections')
        try:
            data = node_connections[successor].recv(4096)
        except socket.timeout, e:
            err = e.args[0]
            if err == 'timed out':
                print(“recv timed out”)
                theMotherFuckerCrashed(node)
            else:
                print(“FUCK ME”)
                print e
                sys.exit(1)
        time.sleep(5)

def receiveHeartbeat(node,message_obj,conn):
    num = getattr(node,'num')
    print(“Node “ + str(num) + “ got heartbeat from “ + str(message_obj['sourcenum']))
    predecessor = message_obj['sourcenum']
    msg = {
        'source': 'Node',
        'sourcenum': getattr(node,'num'),
        'action': 'Im alive motherfucker'
    }
    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)#send message back to predecessor saying alive

def theMotherFuckerCrashed(node):#finger tables, keys, predecessor keys
    successor = getattr(node,'myFingerTable')[0]#this crashed
    setMyFingerTable(node)#updates my finger table so myFingerTable[0] equals new successor
    setSuperSuccessor(node)#sets my supersuccessor using myFingerTable[0]
    setPredecessorSuperSuccessor(node)#sets predecessors supersuccessors



    chnageFingerTables(node,successor)# now update everyone elses finger table


    successor = getattr(node,'myFingerTable')[0]
    predecessor = getattr(node,'myPredecessor')
    num = getattr(node, 'num')
    myKeys = getattr(node,'myKeys')
    if(successor == predecessor):#only 3 keys in the system, 1 crashed, so now only 2 keys
        print(“”)
    elif(successor == num):#only 2 keys in the system, 1 crashed, only node 0 remains
        print(“”)
    msg = {
            'source': “Node”,
            'action': 'Crashed Key',
            'sourcenum': num,
            'absorbBackupKeys': True,#successor should absorb its backup keys
            'backupKeys': myKeys # set its new backupkeys to my keys
    }
    sendNode2NodeMessage(node,msg,successor)

def changeFingerTables(node,successor):
    num = getattr(node,'num')
    myFingerTable = getattr(node,'myFingerTable')
    newsuccessor = myFingerTable[0]
    for i in range(8):
        predecessorValue = (num - (2**i)) % (2**8)
        predecessorNode = findPredecessor(node, predecessorValue)
        crashFingerTableUpdate(node, predecessorNode, successor, newsuccessor)# if successor is in your finger tabel update to new successor


def crashFingerTableUpdate(node, predecessorNode, successor, newsuccessor):
    msg = {
        'source': 'Node',
        'action': 'receiveFingerTableUpdate',
        'successor': successor,
        'newsuccessor': newsuccessor
    }
    sendNode2NodeMessage(node, msg, getattr(predecessorNode, 'num'))

def receiveFingerTableUpdate(node, message_obj):
    myFingerTable = getattr(node,'myFingerTable')
    successor = message_obj['successor']
    newsuccessor = message_obj['newsuccessor']
    for i in range(len(myFingerTable)):
        if(myFingerTable[i] == successor):
            myFingerTable[i] = newsuccessor

def setPredecessorSuperSuccessor(node):#send message to my predecessor with my new successor so it can update its supersuccessor
    successor = getattr(node,'myFingerTable')[0]
    predecessor = getattr(node,'myPredecessor')
    msg = {
        'successor': successor
    }
    sendNode2NodeMessage(node,msg,myPredecessor)
def getSuperSuccessor(node,message_obj):#this is when predecessor gets message
    setattr(node,'supersuccessor',message_obj['successor'])


def setSuperSuccessor(node):#send message to my new successor asking for his successor so i can set it to my new supersuccessor
    successor = getattr(node,'myFingerTable')[0]
    msg = {
        'source': 'Node',
        'action': 'Get Successor',
        'predecessor': getattr(node, 'num')
    }
    sendNode2NodeMessage(node,msg,successor)
    data = node_connections[successor].recv(4096)
    response = pickle.loads(data)
    setattr(node,'superSuccessor',response['successor'])


def setMyFingerTable(node):#sets my finger table
    myFingerTable = getattr(node,'myFingerTable')
    superSuccessor = getattr(node, 'superSuccessor')
    crashed = myFingerTable[0]
    for i in range(len(myFingerTable)):
        if(myFingerTable[i] == crashed):
            myFingerTable[i] = superSuccessor

def crashedKeyChanges(node,message_obj):
    myKeys = getattr(node,'myKeys')
    myPredecessorKeys = getattr(node,'myPredecessorKeys')
    if(message_obj['absorbBackupKeys'] == True):
        for i in reversed(len(range(myPredecessorKeys))):# doing this to make sure keys stays in sorted order
            myKeys.insert(0,myPredecessorKeys[i])
    setattr(node,'myPredecessorKeys',msg['backupKeys'])# set new backup keys to keys of predecessor




''' ========= Implementing moving keys =========== '''

'''
'''

def moveKeys(node):# move keys in (predecessor,node] from successor
    myPredecessor = getattr(node,'myPredecessor')#
    msg = {
        'source': “Node”,
        'action': 'Take Keys',
        'node_num': getattr(node,'num'),
        'predecessor': myPredecessor,
    }
    successor = getattr(node,”myFingerTable”)[0]

    # print(str(getattr(node, 'num')) + ' requesting keys from ' + str(successor))

    sendNode2NodeMessage(node, msg, successor)#sends message to successor node asking to take keys

    node_connections = getattr(node, 'node_connections')
    data = node_connections[successor].recv(4096)

    response = pickle.loads(data)#should get the keys back
    print(“Take Keys: “ + str(response))

    keys = response['keys']
    # print(str(getattr(node, 'num')) + ' received keys from ' + str(successor) + ': ' + str(keys))
    setattr(node,'myKeys',keys)#set your keys to the incoming keys
    setattr(node, 'myPredecessorKeys', response['predecessorKeys'])


def takeKeys(node,message_obj, conn):
    # print(str(getattr(node, 'num')) + ' received request to take keys')

    myKeys = getattr(node,'myKeys')
    backupKeys = []
    predecessor = message_obj['predecessor']
    num = message_obj['node_num']
    for i in range(len(myKeys)):
        if(myKeys[i]>predecessor and myKeys[i]<=num):
            backupKeys.append(myKeys[i])#these are keys being sent to joining node but are also my nodes predecessor keys

    for i in reversed(range(len(myKeys))):
        if(myKeys[i]>predecessor and myKeys[i]<=num):
            myKeys.remove(myKeys[i])

    predecessorKeys = getattr(node,'myPredecessorKeys')


    setattr(node,'myKeys',myKeys)#set keys to new keys
    setattr(node,'myPredecessorKeys',backupKeys)# these will also be sent back to the node that asked for keys
    successor = getattr(node,'myFingerTable')[0]
    msg = {#sending to my successor his new backupkeys
        'source': “Node”,
        'action': “Give Predecessor Keys”,
        'predecessorKeys': getattr(node,'myKeys')
    }

    # print(str(getattr(node, 'num')) + ' sending keys back: ' + str(backupKeys))

    serialized_message = pickle.dumps(msg, -1)
    sendNode2NodeMessage(node,msg,successor)
    msg = {#sending to the entering nodes his keys and his backupkeys
        'source': “Node”,
        'action': 'Give Keys',
        'keys': backupKeys,
        'predecessorKeys': predecessorKeys
    }

    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)
    # sendNode2NodeMessage(node, msg, int(num))#sends message back to predecessor that asked for keys

'''
node joins the network
cur_node is an arbitrary node in the network (will be 0)
'''
def join(cur_node, node, conn):
    # print(“join: Trying to join “ + str(getattr(node, 'num')) + “ to chord”)

    fingers = getattr(node, 'myFingerTable')
    # print(“join: “ + str(fingers))

    initializeFingerTable(cur_node, node)

    fingers = getattr(node, 'myFingerTable')
    # print(“join: After initializing finger table: “ + str(fingers))

    msg = {
        'source': 'Node',
        'action': 'Joined node to chord',
        'num': getattr(cur_node, 'num'),
        'node': node
    }

    msg = removeSocketsFromMessage(msg)

    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)


    # print(“join: Responded to join request”)

    # sendNode2NodeMessage(cur_node, msg, getattr(node, 'num'))

'''
Initialize the finger table of node
cur_node is an arbitrary node in the network
'''
def initializeFingerTable(cur_node, node):
    # print(“initializeFingerTable: Start”)
    nodeFingerTable = getattr(node, 'myFingerTable')
    node_num = getattr(node, 'num')

    # Set finger[0]
    finger_start = (node_num + 2**0) % (2**8)
    # print(“initializeFingerTable: Trying to find successor”)
    successor = findSuccessor(cur_node, finger_start)
    nodeFingerTable[0] = getattr(successor, 'num')
    # print(“initializeFingerTable: Found successor = “ + str(nodeFingerTable[0]))

    # predecessor = successor.predecessor
    setattr(node, 'myPredecessor', getattr(successor, 'myPredecessor'))

    # successor.predecessor = n
    setattr(node, 'myFingerTable', nodeFingerTable)
    setSuccessorPredecessor(cur_node, node)

    for i in range(7):
        finger_start = (node_num + 2**(i+1)) % (2**8)
        # print(str(i) + “: “ + str(finger_start))
        # if (node_num <= finger_start < nodeFingerTable[i]):
        if (checkInterval(node_num, finger_start, nodeFingerTable[i], True, False)):
            # print(“initializeFingerTable: Node “ + str(node_num) + “: Setting “ + str(i+1) + “ to “ + str(nodeFingerTable[i]))
            nodeFingerTable[i+1] = nodeFingerTable[i]
        else:
            nodeFingerTable[i+1] = getattr(findSuccessor(cur_node, finger_start), 'num')
            # print(“initializeFingerTable: Node “ + str(node_num) + “: Setting “ + str(i+1) + “ to “ + str(nodeFingerTable[i+1]))
    setattr(node, 'myFingerTable', nodeFingerTable)
    # print(“initializeFingerTable: End”)

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
        }

        sendNode2NodeMessage(cur_node, msg, successor)
    else:
        setattr(cur_node, 'myPredecessor', getattr(node, 'num'))
        # print(“setSuccessorPredecessor: “ + str(getattr(cur_node, 'num')) + “'s predecessor is now “ + str(getattr(cur_node, 'myPredecessor')))


''' -- Updating others finger tables -- '''

'''
Updates all nodes whose finger tables should refer to n
'''
def updateOthers(node):
    num = getattr(node, 'num')
    # print(“updateOthers: Start”)
    for i in range(8):
        # Find the last node whose ith finger might be node
        predecessorValue = (num - (2**i)) % (2**8)
        print(str(num) + “'s updateOthers: Before finding predecessorNode of “ + str(predecessorValue))
        predecessorNode = findPredecessor(node, predecessorValue)
        print(str(num) + “'s updateOthers: Found “ + str(i) + “th predecessor node: “ + str(getattr(predecessorNode, 'num')) )

        # Send a message to the predecessor to update its finger table
        sendUpdateFingerMessage(node, predecessorNode, num, i)

        # updateFingerTable(predecessorNode, node, i)
    # print(“updateOthers: End”)

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

    # print(“sendUpdateFingerMessage: “ + str(getattr(predecessorNode, 'num')) + ', ' + str(s) + ', ' + str(i))

    sendNode2NodeMessage(node, msg, getattr(predecessorNode, 'num'))

'''
If node s is the ith finger table entry of node n, update n's finger table with s
'''
def updateFingerTable(node, s, i):
    num = getattr(node, 'num')

    if (s != num):
        myFingerTable = getattr(node, 'myFingerTable')
        my_finger_temp = myFingerTable[i]
        if (checkInterval(num, s, myFingerTable[i], True, False)):
            # print(str(num) + “ updateFingerTable: Setting “ + str(i) + “ to “ + str(s))
            myFingerTable[i] = s
            setattr(node, 'myFingerTable', myFingerTable)

            p = requestNodeInfo(node, getattr(node, 'myPredecessor'))
            sendUpdateFingerMessage(node, p, s, i)

'''
Returns the information of the successor of value
'''
def findSuccessor(node, value):
    # print(“findSuccessor: “ + str(value))
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
    while (not checkInterval(num, value, successor, False, True)):
        print(str(num) + “'s findPredecessor: “ + str(num) + “ < “ + str(value) + “ <= “ + str(successor))
        predecessorNode = getClosestPrecedingFinger(node, predecessorNode, value)
        # Check for infinite loop
        if (getattr(predecessorNode, 'num') == num):
            print('findPredecessor: Requesting same node ' + str(num))
            break
        num = getattr(predecessorNode, 'num')
        successor = getattr(predecessorNode, 'myFingerTable')[0]
    return predecessorNode

def sendMySuccessor(node,conn,message_obj):
    if('predecessor' in message_obj):#if this is called from crash it will have this
        setattr(node,'myPredecessor',message_obj['predecessor'])
    successor = getattr(node,'myFingerTable')[0]
    msg = {
        'source': 'Node',
        'successor': successor
    }

    print('sendMySuccessor (' + str(getattr(node, 'num')) + '): ' + str(msg))
    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)



def checkInterval(beg,num,end,beginclusive,endinclusive):
    if(beginclusive and endinclusive):
        if(beg < end):
            return (beg <= num <= end)
        else:
            return (beg <= num or num <= end)
    elif(beginclusive and (not endinclusive)):
        if(beg < end):
            return (beg <= num < end)
        else:
            return (beg <= num or num < end)
    elif(endinclusive and (not beginclusive)):
        if(beg < end):
            return (beg < num <= end)
        else:
            return (beg < num or num <= end)
    else:
        if(beg < end):
            return (beg < num < end)
        else:
            return (beg < num or num < end)

'''
Returns the closest finger preceding the node of value
'''
def getClosestPrecedingFinger(cur_node, node, value):
    num = getattr(node, 'num')
    # print(“num = “ + str(num) + “, value = “ + str(value))

    myFingerTable = getattr(node, 'myFingerTable')
    # print(“getClosestPrecedingFinger: “ + str(myFingerTable))
    for i in reversed(range(8)):
        print(str(num) + “'s getClosestPrecedingFinger: “ + str(num) + “ < “ + str(myFingerTable[i]) + “ < “ + str(value))
        # if (num < myFingerTable[i] < value):
        if (checkInterval(num, myFingerTable[i], value, False, False)):
            print(str(num) + “'s getClosestPrecedingFinger: Found “ + str(myFingerTable[i]))
            return requestNodeInfo(cur_node, myFingerTable[i])
    # else:
    # for i in reversed(range(8)):
    # print(str(num) + “'s getClosestPrecedingFinger: “ + str(value) + “ < “ + str(myFingerTable[i]) + “ < “ + str(num))
    # if (not (value <= myFingerTable[i] <= num)):
    # print(str(num) + “'s getClosestPrecedingFinger: Found “ + str(myFingerTable[i]))
    # return requestNodeInfo(node, myFingerTable[i])

    return node

'''
Sends a message to the node with value to request its information
'''
def requestNodeInfo(node, value):
    myNum = getattr(node, 'num')
    # print(str(myNum) + “'s requestNodeInfo: “ + str(myNum) + ' requesting info from ' + str(value))

    if (myNum != value):

        msg = {
            'source': 'Node',
            'action': 'Requesting node info',
            'num': myNum
        }

        print(“requestNodeInfo (“ + str(myNum) + “): Sending message to “ + str(value))
        sendNode2NodeMessage(node, msg, value)

        node_connections = getattr(node, 'node_connections')

        data = node_connections[value].recv(4096);
        response = pickle.loads(data)

        print(“requestNodeInfo (“ + str(myNum) + '): Received info from ' + str(value) + ' : ' + str(response))

        # print('requestNodeInfo: ' + str(response))

        return response['node']

    # print(str(myNum) + “'s requestNodeInfo: Requesting from myself!”)
    return node

'''
Sends the current node info back to the requesting node
'''
def sendNodeInfo(node, num, conn):
    msg = {
        'source': 'Node',
        'action': 'Responding node information',
        'node': node
    }
    # print('Before removing sockets: ' + str(msg))

    msg = removeSocketsFromMessage(msg)

    # print('After removing sockets: ' + str(msg))

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
        if (hasattr(msg_no_sockets['node'], 'client_socket')):
            del msg_no_sockets['node'].client_socket
    # setattr(msg['node'], 'node_connections', None)
    # setattr(msg['node'], 'socket', None)
    return msg_no_sockets

'''
Sends a message to another node given the message and the node's id
'''
def sendNode2NodeMessage(node, msg, num):
    #time.sleep((random.uniform(min_delay, max_delay)/1000.0))
    node_connections = getattr(node, 'node_connections')
    # if (node_connections is None):
    # node_connections = {}

    if (not (num in node_connections)):
        node_connections[num] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_connections[num].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = num + 2000
        try:
            # print('sendNode2NodeMessage: ' + str(getattr(node, 'num')) + ' making connection to ' + str(num))
            # print('sendNode2NodeMessage: connecting at port ' + str(port))
            node_connections[num].connect((“127.0.0.1”, port))
            setattr(node, 'node_connections', node_connections)

            if ('node' in msg):
                msg = removeSocketsFromMessage(msg)

        except Exception, e:
            print(“sendNode2NodeMessage: “ + str(e))

    serialized_message = pickle.dumps(msg, -1)
    node_connections[num].sendall(serialized_message)


    if(msg['action'] == 'Heartbeat'):
        node_connections[num].settimeout(5)#set socket to timeout after 5 seconds

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
        self.client_socket = -1 # connection to client
        self.myPredecessor = -1
        self.socket = -1
        self.superSuccessor = -1

        self.setup_node(port)

        Node.count += 1

    def create_server(self, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # print(“Trying to connect to port “ + str(port))
        s.bind((“127.0.0.1”, port))
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
if __name__ == “__main__”:
    signal.signal(signal.SIGINT, signal_handler)
    if (len(sys.argv) != 1):
        print(“python “ + sys.argv[0])
    else:
        main()