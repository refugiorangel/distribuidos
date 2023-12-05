from datetime import datetime
import socket
import threading
import time
import queue
import pandas
import random
activeMachines = []
allMachines = ["30","31","32","33","34","35"]
port = 1234
localIP = socket.gethostbyname(socket.gethostname()).split["."][-1]
masterNode = ""
ipBase = socket.gethostbyname(socket.gethostname())
nodeAvilable = queue.Queue()

for i in range(len(ipBase) -1, -1, -1):
    if ipBase[i] == ".":
        ipBase = ipBase[:i+1]
        break

def analizedMessage(message):
    tokens = message.split(" ")
    global masterNode
    global activeMachines
    global localIP
    global nodeAvilable
    
    match tokens[0]:
        case "A0":
            print(activeMachines)
            if tokens[1] not in activeMachines:
                activeMachines.append(tokens[1])
            print(activeMachines)

        case "A1":
            print(masterNode)
            print(activeMachines)
            if tokens[1] == masterNode:
                masterNode = ""
                index = activeMachines.index(tokens[1])
                activeMachines.remove(index)
            print(activeMachines)
        
        case "A2":
            if masterNode != "":
                print(masterNode)
                print(activeMachines)
                try:
                    index= activeMachines.index(masterNode)
                    activeMachines.remove(index)
                except:
                    pass
            masterNode = tokens[1]
            print(masterNode)
            print(activeMachines)

        case "A3":
            if localIP > tokens[1]:
                activeMachines.sort()
                if((len(activeMachines) -1) == activeMachines.index(localIP)):
                    next = activeMachines[0]
                else:
                    next = activeMachines[activeMachines.index(localIP) + 1]
                sendMessage(next, f"A3 {localIP}")
            else:
                sendAll(f"A2 {localIP}")

        case "A4":
            pass
            #agregar maquina al queue

        case "FF":
            pass

def activeServer():
    global ipBase
    global port
    global localIP
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((ipBase + localIP, port))
    server.listen(5)
    print(f"LISTENING ON {localIP}")

    while True:
        conn, cliente = server.accept()
        print(f"NEW CONNECTION FROM {cliente[0]}")
        message = conn.recv(1024).decode('utf-8')
        if message != "":
            print(f"RECEIVEC {message} from {cliente[0]}")
            analizedMessage(message)
            response = "FF"
            conn.send(response.encode('utf-8'))
        conn.close()
    server.close()

def sendMessage(ip, message):
    global ipBase
    global port
    print(f"SENDING {message} to {ip}")
    try:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.connect((ipBase + ip, port))
        cliente.send(message.encode('utf-8'))
        response = cliente.recv(1024).decode('utf-8')
        analizedMessage(response)
        print(f"SEND {message} to {ip} SUCESS!!!")
    except:
        print(f"SEND {message} to {ip} FAILED!!!")

def getActives():
    global allMachines
    global ipBase
    global port
    global activeMachines

    print(f"GETTING ALL ACTIVE MACHINES")
    for i in allMachines:
        if i == localIP:
            pass
        else:
            try:
                cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                cliente.connect((ipBase + i, port))
                cliente.close()
                activeMachines.append(i)
                print(f"{i} ADDED TO ACTIVE MACHINES DIRECTORY")
            except:
                print(f"{i} FAILED CONNECTION")
    print(activeMachines)

def sendAll(*message):
    global activeMachines
    global ipBase
    inicial = activeMachines

    if(len(message) == 1):
        for i in inicial:
            try:
                sendMessage(i, message[0])
            except:
                index = activeMachines.index(i)
                activeMachines.remove(index)
                sendAll(f"A1 {i}")
    else:
        for i in inicial:
            try:
                sendMessage(i, message[1] + " "+message[0])
            except:
                index = activeMachines.index(i)
                activeMachines.remove(index)
                sendAll(f"A1 {i}")

def sendActive():
    global masterNode
    global localIP
    global activeMachines

    print(threading.active_count())
    if(threading.active_count() == 1):
        try:
            sendMessage(masterNode, "A4")
        except:
            index = activeMachines.index(i)
            activeMachines.remove(index)
            sendAll(f"A1 {masterNode}")
            activeMachines.sort()
            if((len(activeMachines) -1) == activeMachines.index(localIP)):
                next = activeMachines[0]
            else:
                next = activeMachines[activeMachines.index(localIP) + 1]
            sendMessage(next, f"A3 {localIP}")
    else:
        pass

typeAction = {
    "00": {
        "function": getActives,
        "def": "Retorna una lista con las maquinas activas"
    },

    "01": {
        "function": sendActive,
        "def": "Manda un mensaje para indicar que esta activo y disponible"
    },

    "02": {
        "function": sendMessage,
        "def": "Manda un mensaje a cualquier ip"
    },

    "03": {
        "function": sendAll,
        "def": "Manda un mensaje a todos los nodos activos"
    }
}

server_t = threading.Thread(target=activeServer)
server_t.start()
print(f"WRITE AN ACCION FOR START")

while True:
    t = input()
    accion = typeAction[t]["function"]

    match t:
        case "02":
            inp = input("IP,MESSAGE:")
            sp = inp.split(",")
            action_t = threading.Thread(target=accion, args=(sp[0], sp[1]))
            action_t.start()

        case "03":
            inp = input("MESSAGE:")
            action_t = threading.Thread(target=accion, args=inp)
            action_t.start()

        case _:
            action_t = threading.Thread(target=accion)
            action_t.start()
