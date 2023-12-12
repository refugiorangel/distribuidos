from datetime import datetime
import socket
import threading
import time
import queue
import pandas as pd
import random
import paramiko
import os

activeMachines = []
allMachines = ["30","31","32","33","34","35","36"]
port = 1234
localIP = socket.gethostbyname(socket.gethostname()).split(".")[-1]
masterNode = ""
ipBase = socket.gethostbyname(socket.gethostname())
nodeAvilable = queue.Queue()

for i in range(len(ipBase) -1, -1, -1):
    if ipBase[i] == ".":
        ipBase = ipBase[:i+1]
        break

def analizedMessage(message,conn):
    tokens = message.split(" ")
    global masterNode
    global activeMachines
    global localIP
    global nodeAvilable

    match tokens[0]:
        #agrga una maquina activa
        case "A0":
            print(activeMachines)
            if tokens[1] not in activeMachines:
                activeMachines.append(tokens[1])
            print(activeMachines)

        #remueve el nodo maestro
        case "A1":
            print(masterNode)
            print(activeMachines)
            if tokens[1] == masterNode:
                masterNode = ""
                index = activeMachines.index(tokens[1])
                activeMachines.remove(index)
            print(activeMachines)

        #agrega masternode
        case "A2":
            print(masterNode)
            print(activeMachines)
            if masterNode != "":
                try:
                    index= activeMachines.index(masterNode)
                    activeMachines.remove(index)
                except:
                    pass
            masterNode = tokens[1]
            activeMachines.append(masterNode)
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
            if tokens[1] not in nodeAvilable.queue:
                nodeAvilable.put(tokens[1])
            print(nodeAvilable.queue)
            #agregar maquina al queue

        case "A5":
            path = f"/home/adm-user/proyecto/distribuidos/data/{tokens[1]}"
            with open(path, 'wb') as file:
                data = conn.recv(1024).decode('utf-8')
                while data:
                    file.write(data)
                    data = conn.recv(1024).decode('utf-8')
            # Cierra el socket del servidor
            print("MESSAGE RECEIVED CORRECT!!")

        case "FF":
            pass

f = open("")
f.read
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
        message = conn.recv(5).decode('utf-8')
        if message != "":
            print(f"RECEIVEC {message} from {cliente[0]}")
            analizedMessage(message, conn)
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
        analizedMessage(response, cliente)
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

    if(threading.active_count() == 3):
        try:
            sendMessage(masterNode, f"A4 {localIP}")
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

def defineMaster(ip):
    global masterNode
    print(masterNode)
    masterNode = ip
    print(masterNode)

def initialDistribution():
    global activeMachines  
    global localIP  
    df = pd.read_csv("/home/adm-user1/proyecto/distribuidos/data/PRODUCTOS.csv")
    if len(activeMachines) >0:
        df["Existencias"] = df["Existencias"] // (len(activeMachines)+1)
        df["Exceso"] = df["Existencias"] % (len(activeMachines)+1)
        pf = df[["ItemID","ItemBarcode", "ItemName", "Price", "Cost", "Categoria", "Existencias"]]

        for i in activeMachines:
            pf.to_csv(f"/home/adm-user1/proyecto/distribuidos/data/{i}.csv", index=False)
        pf.to_csv(f"/home/adm-user1/proyecto/distribuidos/data/{localIP}.csv", index=False)
    else:
        df.to_csv(f"/home/adm-user1/proyecto/distribuidos/data/{localIP}.csv", index=False)
    print("DONE")

def copyFile(ip, file):
    global ipBase
    global port
    global localIP

    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sendSocket.connect((ipBase + ip, port))
    message = f"A5 {file}"
    sendSocket.send(message.encode('utf-8'))
    path = f"/home/adm-user1/proyecto/distribuidos/data/{file}"
    with open(path, 'rb') as f:
        # Envía el archivo al servidor
        data = f.read(1024)
        while data:
            sendSocket.send(data)
            data = f.read(1024)

    print(f"Archivo {file} enviado con éxito")
    sendSocket.close()

def sendFiles():
    global activeMachines
    global ipBase
    global localIP
    files = os.listdir("/home/adm-user1/proyecto/distribuidos/data/")

    for i in activeMachines:
        for j in files:
            copyFile(i, j)
    print("ALL FILES SENDED")

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
    },

    "04": {
        "function": initialDistribution,
        "def": "Hace la distribución inicial"
    },

    "05": {
        "function": defineMaster,
        "def": "define nodo maestro"
    },

    "06": {
        "function": copyFile,
        "def": "copia un archivo a otra maquina"
    },
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
        
        case "05":
            inp = input("Master Node:")
            action_t = threading.Thread(target=accion, args=(inp,))
            action_t.start()

        case "06":
            inp = input("IP,FILE:")
            sp = inp.split(",")
            action_t = threading.Thread(target=accion, args=(sp[0], sp[1]))
            action_t.start()

        case _:
            action_t = threading.Thread(target=accion)
            action_t.start()
