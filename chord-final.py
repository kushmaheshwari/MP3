import sys
import signal
from threading import Thread, Lock, Condition
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
go_go_go = False
count = 0

min_delay = 0
max_delay = 0

cv = Condition()
mutex = Lock()

clientCV = Condition()
client_GO = False

joins = []

message_count = 0

join_count = 0
find_count = 0


# x = -1

# Ran with commands "python client.py"
def main():
    global go_go_go
    parse_file()
    parse_outputFiles()

    client_thread = Thread(target=setup_client, args = ())
    client_thread.daemon = True
    client_thread.start()

    while True:
        time.sleep(100)

'''
Sets up the client for reading input from the command line
And sets up node 0 in the server
'''

def parse_outputFiles():
    global joins
    with open('output_P20_1.txt') as f:
        for line in f:
            joins.append(line)

    with open('output_P20_2.txt') as f:
        for line in f:
            joins.append(line)

    joins.append("show all")

def setup_client():
    # Create node 0
    create_node = Thread(target=setup_node, args = (0,port))#creating node 0
    create_node.daemon = True
    create_node.start()

    count = 0

    flag = False
    global join_count
    global find_count

    while(clientToNode(0, port) == False):
        # print("Trying to connect to node 0")
        time.sleep(0.5)
    while True:
        invalid_command = False
        user_input = raw_input('')
        # user_input = joins[count]
        # print(user_input)
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
                    if (node_num in client_connections):
                        print("Node " + input_split[1] + " already exists.")
                    elif (node_num > 255):
                        print('Invalid node ID. Node ID must be [0, 255].')
                    else:
                        newport = port + node_num
                        create_node = Thread(target=setup_node, args = (node_num, newport))
                        create_node.daemon = True
                        create_node.start()
                        while(clientToNode(node_num, int(newport)) == False):
                            # print("setup_client: Trying to connect to node " + input_split[1])
                            time.sleep(1)

                        socket = client_connections.get(node_num)
                        data = socket.recv(4096)
                        response = pickle.loads(data)
                        print('Node ' + str(response['num']) + ' has joined the system.')

                # Finds where a key is stored
                elif(len(input_split) > 2 and input_split[0] == "find" and isDigit and input_split[2].isdigit()):
                    keynum = int(input_split[2])
                    msg = {
                        'source': "client",
                        'action': 'Find key',
                        'keynum': keynum,
                        'message': "Find key"
                    }
                    socket = client_connections.get(node_num)
                    serialized_message = pickle.dumps(msg,-1)
                    socket.sendall(serialized_message)

                    # print("Client waiting for response")
                    data = socket.recv(4096)
                    # print("Client received response")
                    response = pickle.loads(data)

                    if (response['node_num'] != -1):
                        print("Key " + str(keynum) + " found at Node " + str(response['node_num']))
                    else:
                        print("Key " + str(keynum) + " not found.")
                # Clean crashes a node
                elif(input_split[0] == "crash" and isDigit):
                    print("Crashing node " + input_split[1])
                    msg = {
                        'source': "client",
                        'message': 'Crash',
                        'action': 'Crash'
                    }
                    socket = client_connections.get(node_num)
                    serialized_message = pickle.dumps(msg,-1)
                    socket.sendall(serialized_message)

                    socket.close()
                    del client_connections[node_num]

                    # global x
                    # x = node_num

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

                        data = socket.recv(4096)
                        response = pickle.loads(data)

                        clientPrintShow(response)

                # Show all nodes' information
                elif(input_split[0] == "show" and input_split[1] == "all"):
                    q = Q.PriorityQueue()

                    # Send and receive show request to all nodes
                    msg = {
                        'source': "client",
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

                    # print("Showing all nodes info")
                else:
                    invalid_command = True
            else:
                invalid_command = True
        if (invalid_command):
            print("Invalid command. Valid commands are:")
            print("join <p>, find <p> <k>, crash <p>, show <p>, show all")

        clientCV.acquire()
        while(not client_GO):
            clientCV.wait()
        count+=1
        if(count == len(joins)):
            print("Join Count: " + str(join_count))
            print("Find Count: " + str(find_count))
            sys.exit(0)
        command = joins[count].split()
        if(command[0] == 'find' and flag == False):
            flag = True
            join_count = message_count
        if(command[0] == 'show'):
            find_count = message_count - join_count
        clientCV.release()
        # time.sleep(4)





'''
Prints out what a node returned on show on the client side
'''
def clientPrintShow(msg):
    print('====== Showing Node ' + str(msg['num']) + ' ======')
    print('Finger table: ' + str(msg['myFingerTable']))
    print('Keys: ' + str(msg['myKeys']))
    # print('PredecessorKeys: ' + str(msg['myPredecessorKeys']))
    print('Predecessor: ' + str(msg['myPredecessor']))
    print('Super-Successor: ' + str(msg['superSuccessor']))
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

        msg = {
            'source': "client",
            'message': "Hi",
            'action': "Hi",
        }
        serialized_message = pickle.dumps(msg, -1)
        s.sendall(serialized_message)
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
        try:
            conn, addr = socket.accept()
        except:
            break
        conn_thread = Thread(target = readMessages, args = (conn,node))
        conn_thread.start()

'''
Server of the node that reads messages sent to the node
Depending on the action of the message, executes commands
'''
def readMessages(conn,node):
    num = getattr(node, 'num')

    while True:
        try:
            dump = conn.recv(4096)
        except Exception, e:
            pass
        if (not dump):
            break

        message_obj = pickle.loads(dump)

        # print("readMessages (" + str(getattr(node, 'num')) + '): ' + str(message_obj))

        if(message_obj['source'] == "client"): # message from client
            if (getattr(node, 'client_socket') == -1):
                setattr(node, 'client_socket', conn)

            message = message_obj['message'].split()
            # Show message
            if (message[0] == "show"):
                showMine(node, conn)
            # Find key
            elif (message_obj['action'] == 'Find key'):#node that recieves request to find a key
                if ('predecessor' in message_obj):
                    node_connections = getattr(node, 'node_connections')
                    node_connections[num] = conn
                    setattr(node, 'node_connections', node_connections)
                findNodeRequest(node,message_obj,conn)
            # Crash current node
            elif (message[0] == "Crash"):
                node.destroy()
                return

        else: # message from node

            # Delay message
            time.sleep(random.uniform(min_delay, max_delay))

            # print("readMessages: " + str(getattr(node, 'num')) + " received a message: " + message_obj['action'])
            # Set your predecessor to node
            if (message_obj['action'] == 'Set predecessor'):
                setattr(node, 'myPredecessor', message_obj['num'])
            # Update your finger table
            elif (message_obj['action'] == 'Update finger table'):
                updateFingerTable(node, message_obj)
            # Node is joining the chord system (only node 0)
            elif (message_obj['action'] == 'Joining chord system'):
                # print("readMessages: " + str(getattr(node, 'num')) + " received a join request")
                join(node, message_obj['node'], conn)
            elif(message_obj['action'] == 'Find Key'):
                if (message_obj['message'] == 'Find successor'):
                    node_connections = getattr(node, 'node_connections')
                    node_connections[num] = conn
                    setattr(node, 'node_connections', node_connections)

                if ('nodes_visited' in message_obj):
                    findNode2(node,message_obj)
                else:
                    findNodeRequest(node, message_obj, conn)
            elif(message_obj['action'] == 'Found Node with Key'):#send to client
                # print("FOUND THE NODE WITH KEY: " + str(message_obj))
                msg = {
                    'source': "Node",
                    'action': "Found Key",
                    'message': message_obj['message'],
                    'keynum': message_obj['keynum'],
                    'node_num': message_obj['node_num']
                }
                if ('predecessor' in message_obj):
                    msg['predecessor'] = message_obj[ 'predecessor']

                # Send information back to client
                serialized_message = pickle.dumps(msg, -1)

                if (msg['message'] == 'Find successor'):
                    node_socket = getattr(node, 'node_connections')[num]

                    try:
                        node_socket.sendall(serialized_message)
                    except Exception, e:
                        print(e)
                else:
                    client_socket = getattr(node, 'client_socket')
                    client_socket.sendall(serialized_message)

            elif(message_obj['action'] == 'Take Keys'):
                takeKeys(node,message_obj, conn)
            elif(message_obj['action'] == 'Give Predecessor Keys'):
                setattr(node, 'myPredecessorKeys', message_obj['predecessorKeys'])
            elif (message_obj['action'] == 'superSuccessor'):
                sendMySuccessor(node, conn, message_obj)
            elif (message_obj['action'] == 'Get Successor'):
                returnSuccessor(node, conn, message_obj)
            elif (message_obj['action'] == 'Set super successor'):
                # print('PREDECESSOR ' + str(num) + ': Setting super successor to : ' + str(message_obj))
                setattr(node, 'superSuccessor', message_obj['successor'])
                if (message_obj['depth'] == 0):
                    setPredecessorSuperSuccessor(node, 1)
            elif(message_obj['action'] == 'setSuccessorPredecessorCrash'):
                # print(str(num) + ' setting predecessor to ' + ))
                setattr(node, 'myPredecessor', message_obj['predecessor'])
            elif(message_obj['action'] == 'SetBackupKeys'):
                setattr(node,'myPredecessorKeys', message_obj['backupkeys'])
            elif(message_obj['action'] == 'Heartbeat'):
                # print("Receiving Message")
                receiveHeartbeat(node,message_obj,conn)
            elif(message_obj['action'] == 'Crashed Key'):
                crashedKeyChanges(node, message_obj)
            elif (message_obj['action'] == 'Update finger table Crashed'):
                updateFingerTableCrashed(node, message_obj)

        # print("readMessages (" + str(getattr(node, 'num')) + ") has handled the request: " + str(message_obj))

'''
Returns the successor information to the requesting node
'''
def returnSuccessor(node, conn, message_obj):
    successor = getattr(node, 'myFingerTable')[0]

    msg = {
        'source': 'Node',
        'action': 'Returning successor',
        'successor': successor,
    }

    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)

'''
Sets the super successor of the node by requesting the successor's successor
'''
def setSuperSuccessor(node):#send message to my new successor asking for his successor so i can set it to my new supersuccessor
    successor = getattr(node,'myFingerTable')[0]
    msg = {
        'source': 'Node',
        'action': 'Get Successor',
    }

    sendNode2NodeMessage(node, msg, successor)

    node_connections = getattr(node, 'node_connections')

    data = node_connections[successor].recv(4096)
    response = pickle.loads(data)

    # print('setSuperSuccessor: (' + str(getattr(node, 'num')) + ') request to ' + str(successor) + ': ' + str(response))

    setattr(node, 'superSuccessor', response['successor'])


def setPredecessorSuperSuccessor(node, depth): #send message to my predecessor with my new successor so it can update its supersuccessor
    successor = getattr(node,'myFingerTable')[0]
    predecessor = getattr(node,'myPredecessor')
    msg = {
        'source': "Node",
        'action': 'Set super successor',
        'successor': successor,
        'depth': depth,
    }
    sendNode2NodeMessage(node, msg, predecessor)

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
        'myPredecessor': getattr(node, 'myPredecessor'),
        'superSuccessor': getattr(node, 'superSuccessor')
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

    joinChordSystem(node) # sets its finger table

    if (node is None):
        print("Node is null after joinChordSystem")


'''
Sets the finger table of the node
Node 0 sets its own finger table
Any other node requests access through node 0
'''
def joinChordSystem(node):
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
        setattr(node, 'superSuccessor', 0)
        crash_thread = Thread(target = sendHeartbeats, args = (node,))#sending out heartbeats to successor
        crash_thread.start()
        return

    # mutex.acquire()
    cv.acquire()

    msg = {
        'source' : "Node",
        'action' : "Joining chord system",
        'node': node,
        'num' : num
    }

    # print("joinChordSystem: Sending message to node 0 requesting access")
    sendNode2NodeMessage(node, msg, 0) #send message to node 0 that you want to be entered into the system
    # print("joinChordSystem: Sent message to node 0, waiting for response now")

    # Wait for response
    node_connections = getattr(node, 'node_connections')
    data = node_connections[0].recv(4096)
    response = pickle.loads(data)
    response_node = response['node']

    # print("joinChordSystem: Received response from node 0")

    fingers = getattr(response_node, 'myFingerTable')
    pred = getattr(response_node, 'myPredecessor')
    # print("Response from node 0: \n\tFingers: " + str(fingers) + ', predecessor: ' + str(pred))

    setattr(node, 'myFingerTable', getattr(response_node, 'myFingerTable'))
    setattr(node, 'myPredecessor', getattr(response_node, 'myPredecessor'))

    updateOthers(node)

    global go_go_go
    while (not go_go_go):
        cv.wait()
    go_go_go = False

    moveKeys(node)
    # print("joinChordSystem: Node " + str(num) + ": After updating others")

    # Getting super successor
    setSuperSuccessor(node)
    setPredecessorSuperSuccessor(node, 0)

    cv.release()

    # Send acknowledgment to client
    msg = {
            'source': 'Node',
            'action': 'Completed join',
            'num': getattr(node, 'num')
    }

    time.sleep(1)

    client_socket = getattr(node, 'client_socket')
    serialized_message = pickle.dumps(msg, -1)
    client_socket.sendall(serialized_message)


    global client_GO
    clientCV.acquire()
    client_GO = True
    clientCV.notify()
    clientCV.release()



    crash_thread = Thread(target = sendHeartbeats, args = (node,))#sending out heartbeats to successor
    crash_thread.start()

    return node

def moveKeys(node):# move keys in (predecessor,node] from successor
    myPredecessor = getattr(node,'myPredecessor')#
    msg = {
        'source': "Node",
        'action': 'Take Keys',
        'node_num': getattr(node,'num'),
        'predecessor': myPredecessor,
    }
    successor = getattr(node,"myFingerTable")[0]

    # print(str(getattr(node, 'num')) + ' requesting keys from ' + str(successor))

    sendNode2NodeMessage(node, msg, successor)#sends message to successor node asking to take keys

    node_connections = getattr(node, 'node_connections')
    data = node_connections[successor].recv(4096)

    response = pickle.loads(data)#should get the keys back
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

    # print(str(getattr(node, 'num')) + ' sending keys back: ' + str(backupKeys))

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
    cur_node_num = getattr(cur_node, 'num')

    # Set finger[0]
    finger_start = (node_num + 2**0) % (2**8)
    # print("initializeFingerTable: Trying to find successor")


    msg = {
        'source': "Node",
        'action': 'Find Key',
        'keynum': finger_start,
        'message': "Find successor",
        'sourcenode': 0,
        'predecessor': -1
    }

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.connect(("127.0.0.1", cur_node_num + 2000))
    except:
        print('Could not connect to socket!! :(')

    serialized_message = pickle.dumps(msg,-1)
    s.sendall(serialized_message)

    mutex.acquire()
    global message_count
    message_count += 1
    mutex.release()

    # print('initializeFingerTable: Waiting for response!')
    data = s.recv(4096)
    response = pickle.loads(data)

    node_connections = getattr(cur_node, 'node_connections')
    node_connections[cur_node_num].close()
    node_connections.pop(cur_node_num, None)
    setattr(cur_node, 'node_connections', node_connections)

    nodeFingerTable[0] = response['node_num']
    # print("initializeFingerTable: Found successor = " + str(nodeFingerTable[0]))

    # predecessor = successor.predecessor
    # print('Returned predecessor is HOLLLALALALA: ' + str(response))
    setattr(node, 'myPredecessor', response['predecessor'])

    # successor.predecessor = n
    setattr(node, 'myFingerTable', nodeFingerTable)
    # setSuccessorPredecessor(cur_node, node)

    for i in range(7):
        finger_start = (node_num + 2**(i+1)) % (2**8)
        # print(str(i) + ": " + str(finger_start))
        # if (node_num <= finger_start < nodeFingerTable[i]):
        if (checkInterval(node_num, finger_start, nodeFingerTable[i], True, False)):
            # print("initializeFingerTable: Node " + str(node_num) + ": Setting " + str(i+1) + " to " + str(nodeFingerTable[i]))
            nodeFingerTable[i+1] = nodeFingerTable[i]
        else:
            msg = {
                'source': "Node",
                'action': 'Find Key',
                'keynum': finger_start,
                'message': "Find successor",
                'sourcenode': 0,
            }

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.connect(("127.0.0.1", cur_node_num + 2000))
            except:
                print('Could not connect to socket!! :(')

            serialized_message = pickle.dumps(msg,-1)
            s.sendall(serialized_message)

            data = s.recv(4096)
            response = pickle.loads(data)

            nodeFingerTable[i+1] = response['node_num']

    setattr(node, 'myFingerTable', nodeFingerTable)


'''
=== IMPLEMENTING FIND =====
'''
'''
Node receives a find node request from the client
'''
def findNodeRequest(node,message_obj,conn): #this is for the first node
    keynum = message_obj['keynum']
    myKeys = getattr(node,'myKeys')

    found = False

    for i in range(len(myKeys)):
        if(myKeys[i] == keynum): #found key in this node
            # print("Found key at current node!")
            msg = {
                'source' : "Node",
                'action' : "Found Node with Key",
                'keynum': keynum,
                'message': message_obj['message'],
                'node_num': getattr(node,'num')
            }

            # print('findNodeRequest: Found key at ' + str(getattr(node, 'num')) + ' with predecessor ' + str(getattr(node, 'myPredecessor')))

            if ('predecessor' in message_obj):
                msg['predecessor'] = getattr(node, 'myPredecessor')
                setattr(node, 'myPredecessor', keynum-1)

            try:
                serialized_message = pickle.dumps(msg, -1)
                conn.sendall(serialized_message)#send back to client(need to add clients socket)
            except:
                pass
            found = True
            break

    # Else ask other nodes
    if (not found):
        # forwardFindRequest(node, message_obj)

        myFingerTable = getattr(node, 'myFingerTable')
        queryNode = 999
        for i in range(len(myFingerTable)):#will be a sorted finger table
            if (keynum <= myFingerTable[i] and myFingerTable[i] < queryNode):#might have to do stronger check
                # print(str(keynum) + " is less than " + str(myFingerTable[i]))
                queryNode = myFingerTable[i]#send find to this node
            else:
                break
        if(queryNode == 999):
            queryNode = myFingerTable[0]#set to successor

        msg = {
            'source' : "Node",
            'action' : "Find Key",
            'keynum' : message_obj['keynum'],
            'message': message_obj['message'],
            'sourcenode': getattr(node,'num'),
            'nodes_visited': 1
        }
        if ('predecessor' in message_obj):
            msg['predecessor'] = message_obj['predecessor']

        sendNode2NodeMessage(node, msg, queryNode)#sends message out to other node
        # print("findNodeRequest (" + str(getattr(node, 'num')) + "): Forwarding find to " + str(queryNode))

def findNode2(node,message_obj):#this is for every other node
    keynum = message_obj['keynum']

    found = False

    # Check if already visited multiple nodes (i.e. key does not exist)
    if (message_obj['nodes_visited'] > 30):
        msg = {
            'source': 'Node',
            'action': "Found Node with Key",
            'keynum': keynum,
            'node_num': -1
        }

        # print('Found key at ' + str(getattr(node, 'num')) + ' with predecessor ' + str(getattr(node, 'myPredecessor')))

        if ('predecessor' in message_obj):
            msg['predecessor'] = message_obj['predecessor']
            # setattr(node, 'myPredecessor', keynum-1)
        sendNode2NodeMessage(node, msg, message_obj['sourcenode'])

    else:
        myKeys = getattr(node,'myKeys')
        for i in range(len(myKeys)):
            if(myKeys[i]==keynum):#found key in this node
                msg = {
                    'source' : "Node",
                    'action' : "Found Node with Key",
                    'keynum': keynum,
                    'message': message_obj['message'],
                    'node_num': getattr(node,'num')
                }
                if ('predecessor' in message_obj):
                    msg['predecessor'] = getattr(node, 'myPredecessor')
                    setattr(node, 'myPredecessor', keynum-1)


                # print('findNode2 (' + str(getattr(node, 'num')) + '): Found key at ' + str(getattr(node, 'num')) + ' with predecessor ' + str(getattr(node, 'myPredecessor')))

                # print('findNode2 (' + str(getattr(node, 'num')) + '): Sending key to ') + str(message_obj['sourcenode'])
                sendNode2NodeMessage(node, msg, message_obj['sourcenode'])#send message back to original noce
                found = True
                break

        if (not found):
            # forwardFindRequest(node, message_obj)

            myFingerTable = getattr(node,'myFingerTable')
            queryNode = 999
            for i in range(len(myFingerTable)):#will be a sorted finger table
                if (keynum <= myFingerTable[i] and myFingerTable[i] < queryNode):#might have to do stronger check
                    # print(str(keynum) + " is less than " + str(myFingerTable[i]))
                    queryNode = myFingerTable[i]#send find to this node
                else:
                    break
            if(queryNode == 999):
                queryNode = myFingerTable[0]#set to successor

            msg = {
                'source' : "Node",
                'action' : "Find Key",
                'keynum' : message_obj['keynum'],
                'message': message_obj['message'],
                'sourcenode': message_obj['sourcenode'],
                'nodes_visited': message_obj['nodes_visited'] + 1
            }
            if ('predecessor' in message_obj):
                msg['predecessor'] = message_obj['predecessor']

            sendNode2NodeMessage(node,msg,queryNode)#send to query node
            # print('findNode2 (' + str(getattr(node, 'num')) + "): Forwarding find to " + str(queryNode))


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
        # print("setSuccessorPredecessor: " + str(getattr(cur_node, 'num')) + "'s predecessor is now " + str(getattr(cur_node, 'myPredecessor')))


''' -- Updating others finger tables -- '''

'''
Updates all nodes whose finger tables should refer to n
'''
def updateOthers(node):
    # print('updateOthers')
    num = getattr(node, 'num')
    # myPredecessor = getattr(node, 'myPredecessor')

    mySuccessor = getattr(node, 'myFingerTable')[0]

    msg = {
        'source': 'Node',
        'action': 'Update finger table',
        'value': num
    }

    # print('(' + str(num) + ') updateOthers: Sending initial update request to ' + str(mySuccessor))

    sendNode2NodeMessage(node, msg, mySuccessor)



'''
Sends a message to node telling it to update its finger table
'''
def sendUpdateFingerMessage(node, mySuccessor, value):
    msg = {
        'source': 'Node',
        'action': 'Update finger table',
        'value': value
    }

    sendNode2NodeMessage(node, msg, mySuccessor)

'''
If node s is the ith finger table entry of node n, update n's finger table with s
'''
def updateFingerTable(node, message_obj):
    num = getattr(node, 'num')
    value = message_obj['value']

    if (value != num):
        myFingerTable = getattr(node, 'myFingerTable')

        for i in range(8):
            finger_start = (num + 2**i) % (2**8)

            # print('(' + str(num) + ') updateFingerTable: ' + str(finger_start) + ' <= ' + str(value) + ' < ' + str(myFingerTable[i]))
            if (finger_start != myFingerTable[i] and checkInterval(finger_start, value, myFingerTable[i], True, False)):
                # print(str(num) + ' updateFingerTable: Setting ' + str(i) + ' entry from ' + str(myFingerTable[i]) + ' to ' + str(value))
                myFingerTable[i] = value
                # set_value = True

        # if (set_value):
        mySuccessor = getattr(node, 'myFingerTable')[0]
        # print('(' + str(num) + ') updateFingerTable: Forwarding message to ' + str(mySuccessor))
        sendNode2NodeMessage(node, message_obj, mySuccessor)
        # else:
            # print(str(num) + ' updateFingerTable: Did not update finger table')
    else:
        global go_go_go
        cv.acquire()
        go_go_go = True
        cv.notify()
        cv.release()



''' ========= Implementing Crash =========== '''

'''
'''

def sendHeartbeats(node):
    while True:
        if (getattr(node, 'socket') is None):
            # print('Catch none socket')
            return

        num = getattr(node,'num')
        # print("NUM: " + str(num))
        successor = getattr(node,'myFingerTable')[0]

        # while(successor == num):#while loop until successor is not yourself. for node 0
        # time.sleep(2)
        # successor = getattr(node,'myFingerTable')[0]
        msg = {
            'source': "Node",
            'sourcenum': num,
            'action': 'Heartbeat'
        }

        # if (x == num):
        # print('x global var thank god')
        # return
        # print("Node " + str(num) + " sending heartbeat " + str(successor))
        try:
            sendNode2NodeMessage(node, msg, successor)
            # print('Sent a heartbeat from ' + str(num) + ' to ' + str(successor))
        except:
            nodeHasCrashed(node)

        node_connections = getattr(node, 'node_connections')
        try:
            node_connections[successor].settimeout(5.0)
            data = node_connections[successor].recv(4096)
            node_connections[successor].settimeout(None)
            response = pickle.loads(data)
            # print(str(num) + ' received a response from ' + str(successor) + ': ' + str(response))
        except socket.timeout, e:
            err = e.args[0]
            nodeHasCrashed(node)
        # print("Heartbeat")
        time.sleep(10)

def receiveHeartbeat(node,message_obj,conn):
    # print("Receiving Message")
    num = getattr(node,'num')
    if (getattr(node, 'socket') is None):
        # print(str(num) + ' receiveHeartbeat: socket is none')
        return
    # if (x == num):
    # print(str(num) + ' receiveHeartbeat: x global var pls')
    # return
    # print("Node " + str(num) + " got heartbeat from " + str(message_obj['sourcenum']))
    predecessor = message_obj['sourcenum']
    msg = {
        'source': 'Node',
        'sourcenum': getattr(node,'num'),
        'action': "I'm alive!"
    }
    serialized_message = pickle.dumps(msg, -1)
    conn.sendall(serialized_message)#send message back to predecessor saying alive

def nodeHasCrashed(node):#finger tables, keys, predecessor keys
    cv.acquire()
    successor = getattr(node,'myFingerTable')[0]#this crashed
    # print(str(getattr(node, 'num')) + " thinks " + str(successor) + " crashed.")
    # print("Crashed Node: " + str(successor))
    setMyFingerTable(node)#updates my finger table so myFingerTable[0] equals new successor

    setSuperSuccessor(node)#sets my supersuccessor using myFingerTable[0]
    setPredecessorSuperSuccessor(node, 1)#sets predecessors supersuccessors
    setSuccessorPredecessorCrash(node)#sets new successors predecessor to be myself

    changeFingerTablesCrash(node,successor)# now update everyone elses finger table

    # print("after updating others")

    # print('Before cond wait')
    global go_go_go
    while (not go_go_go):
        cv.wait()
    go_go_go = False

    # print('After cond wait')

    successor = getattr(node,'myFingerTable')[0]
    predecessor = getattr(node,'myPredecessor')
    num = getattr(node, 'num')
    myKeys = getattr(node,'myKeys')
    msg = {
            'source': "Node",
            'action': 'Crashed Key',
            'sourcenum': num,
            'absorbBackupKeys': True,#successor should absorb its backup keys
            'backupKeys': myKeys # set its new backupkeys to my keys
    }
    sendNode2NodeMessage(node,msg,successor)



    cv.release()

    print("Crash has been handled.")


def setMyFingerTable(node):#sets my finger table
    myFingerTable = getattr(node,'myFingerTable')
    superSuccessor = getattr(node, 'superSuccessor')
    if (getattr(node, 'myPredecessor') == getattr(node, 'myFingerTable')[0]):
        setattr(node, 'myPredecessor', getattr(node, 'num'))
    # print('setMyFingerTable ' + str(getattr(node, 'num')) + ': Before updating table: ' + str(myFingerTable))
    crashed = myFingerTable[0]
    for i in range(len(myFingerTable)):
        if(myFingerTable[i] == crashed):
            myFingerTable[i] = superSuccessor

    # print('setMyFingerTable ' + str(getattr(node, 'num')) + ': After updating table: ' + str(myFingerTable))

def setSuccessorPredecessorCrash(node):
    successor = getattr(node,'myFingerTable')[0]
    num = getattr(node, 'num')
    # predecessor = getattr(node,'myPredecessor')
    msg = {
        'source': "Node",
        'action': "setSuccessorPredecessorCrash",
        'predecessor': num
    }
    sendNode2NodeMessage(node,msg,successor)

def changeFingerTablesCrash(node,successor):
    num = getattr(node, 'num')
    mySuccessor = getattr(node, 'myFingerTable')[0]#replace all "successors" with mySuccessor

    msg = {
        'source': 'Node',
        'action': 'Update finger table Crashed',
        'value': num,
        'crashed': successor,
        'replace': mySuccessor
    }


    if (num != mySuccessor):
        # print('(' + str(num) + ') crashFingerTables: Sending initial update request to ' + str(mySuccessor))
        sendNode2NodeMessage(node, msg, mySuccessor)
    else:
        # print('updateFingerTableCrashed: Go go go')
        global go_go_go
        cv.acquire()
        go_go_go = True
        cv.notify()
        cv.release()


def updateFingerTableCrashed(node, message_obj):
    num = getattr(node, 'num')
    value = message_obj['value']

    if (value != num):
        myFingerTable = getattr(node, 'myFingerTable')

        for i in range(8):
            if(myFingerTable[i] == message_obj['crashed']):
                myFingerTable[i] = message_obj['replace']

        mySuccessor = getattr(node, 'myFingerTable')[0]
        sendNode2NodeMessage(node, message_obj, mySuccessor)

    else:
        # print('updateFingerTableCrashed: Go go go')
        global go_go_go
        cv.acquire()
        go_go_go = True
        cv.notify()
        cv.release()


def crashedKeyChanges(node, msg):
    # print('Received crashed key changes')

    myKeys = getattr(node,'myKeys')
    myPredecessorKeys = getattr(node,'myPredecessorKeys')
    if(msg['absorbBackupKeys'] == True):
        for i in reversed(range(len(myPredecessorKeys))):# doing this to make sure keys stays in sorted order
            myKeys.insert(0,myPredecessorKeys[i])

    setattr(node,'myKeys',myKeys)#sets its own keys
    setattr(node,'myPredecessorKeys',msg['backupKeys'])# set new backup keys to keys of predecessor

    msg = {
        'source': 'Node',
        'action': 'SetBackupKeys',
        'backupkeys': getattr(node,'myKeys')
    }
    successor = getattr(node,'myFingerTable')[0]
    sendNode2NodeMessage(node,msg,successor)

def sendMySuccessor(node, conn, message_obj):
    if ('predecessor' in message_obj):
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

    mutex.acquire()
    global message_count
    message_count += 1
    mutex.release()

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
        self.superSuccessor = -1
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # print("Trying to connect to port " + str(port))
        self.socket.bind(("127.0.0.1", port))

        self.port = port
        self.socket.listen(32) # Max of 31 other nodes + client can connect
        Node.count += 1

    def destroy(self):
        # print('Crashing node and closing socket')
        self.socket.close()
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except:
            # print('Woops')
            pass
        self.socket = None
        for num in self.node_connections:
            self.node_connections[num].shutdown(socket.SHUT_RDWR)
            self.node_connections[num].close()
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
            self.client_socket.close()
        except Exception, e:
            pass
        self.client_socket = None

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.connect(("127.0.0.1", self.port))
        except:
            print('Could not connect after closing!')




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
                min_delay = int(process_info[0])/1000
                max_delay = int(process_info[1])/1000
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