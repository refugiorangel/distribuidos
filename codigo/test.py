from datetime import datetime
import socket
import threading
import time
import queue
import pandas as pd
import random
import paramiko
import os, signal
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
activeMachines = []
allMachines = ["30","31","32","33","34","35","36","37","38","39",]
port = 1234
localIP = socket.gethostbyname(socket.gethostname()).split(".")[-1]
masterNode = ""
ipBase = socket.gethostbyname(socket.gethostname())
nodeAvilable = queue.Queue()

for i in range(len(ipBase) -1, -1, -1):
    if ipBase[i] == ".":
        ipBase = ipBase[:i+1]
        break

def analizedMessage(message,server):
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
            activeMachines.remove(tokens[1])
            print(activeMachines)

        #agrega masternode
        case "A2":
            print(masterNode)
            print(activeMachines)
            masterNode = tokens[1]
            if tokens[1] not in activeMachines and tokens[1] != localIP:
                activeMachines.append(masterNode)
            print(masterNode)
            print(activeMachines)

        case "A3":
            if localIP > tokens[1]:
                if len(activeMachines)==1:
                    sendAll(f"A2 {localIP}")
                    defineMaster(localIP)
                    return
                else:
                    activeMachines.append(localIP)
                    activeMachines.sort()
                    index = activeMachines.index(localIP)
                    if index == len(activeMachines) -1:
                        sendAll(f"A2 {localIP}")
                        defineMaster(localIP)
                        activeMachines.remove(localIP)
                        return
                    else:
                        next = activeMachines(index +1)
                        activeMachines.remove(localIP)
                        sendMessage(next, f"A3 {localIP}")
                        return
            else:
                sendAll(f"A2 {localIP}")
                defineMaster(localIP)
                return

        case "A4":
            if tokens[1] not in nodeAvilable.queue:
                nodeAvilable.put(tokens[1])
            print(nodeAvilable.queue)
            #agregar maquina al queue

        case "A5":
            path = f"/home/adm-user1/proyecto/distribuidos/productos/{tokens[1]}"
            conn,cliente = server.accept()
            with open(path, 'w') as file:
                data = conn.recv(1024)
                while data:
                    file.write(data.decode('utf-8'))
                    data = conn.recv(1024)
            # Cierra el socket del servidor
            print(f"{tokens[1]} RECEIVED CORRECT!!")
            conn.close()

        case "A6":
            path = f"/home/adm-user1/proyecto/distribuidos/productos/{tokens[1]}.csv"
            general = f"/home/adm-user1/proyecto/distribuidos/productos/productos.csv"
            try:
                cantidad = int(tokens[3])
                df = pd.read_csv(path)
                item_id_a_modificar = int(tokens[2])
                indice_fila = df.index[df['ItemID'] == item_id_a_modificar].tolist()[0]
                linea = df[df['ItemID'] == item_id_a_modificar]
                if df.at[indice_fila, 'Existencias'] >=cantidad:
                    df.at[indice_fila, 'Existencias'] = df.at[indice_fila, 'Existencias'] - cantidad
                else:
                    cantidad = df.at[indice_fila, 'Existencias']
                    df.at[indice_fila, 'Existencias'] = df.at[indice_fila, 'Existencias'] - cantidad
                df.to_csv(path, index=False)

                df = pd.read_csv(general)
                item_id_a_modificar = int(tokens[2])
                indice_fila = df.index[df['ItemID'] == item_id_a_modificar].tolist()[0]
                if df.at[indice_fila, 'Existencias'] >= cantidad:
                    df.at[indice_fila, 'Existencias'] = df.at[indice_fila, 'Existencias'] - cantidad
                else:
                    cantidad = df.at[indice_fila, 'Existencias']
                    df.at[indice_fila, 'Existencias'] = df.at[indice_fila, 'Existencias'] - cantidad
                df.to_csv(general, index=False)

                total_costo = float(tokens[3]) * float(linea['Price'])
                f = open("/home/adm-user1/proyecto/distribuidos/movimientos/movimientos.csv", "a")
                f.write(f"{tokens[2]},{tokens[1]},{tokens[4]},{cantidad},{total_costo}\n")
                print("ORDER SELL CORRECT!!!")
            except:
                print("ORDER SELL FAILED!!!")

        case "A7":
            path =    f"/home/adm-user1/proyecto/distribuidos/productos/"
            general = f"/home/adm-user1/proyecto/distribuidos/productos/productos.csv" 
            try:
                cantidad = int(tokens[8]) // (len(activeMachines) +1)
                f = open(general, "a")
                f.write(f"{tokens[1]},{tokens[2]},{tokens[3]} {tokens[4]},{tokens[5]},{tokens[6]},{tokens[7]},{tokens[8]}\n")
                f.close()
                f = open(path + localIP + ".csv", "a")
                f.write(f"{tokens[1]},{tokens[2]},{tokens[3]} {tokens[4]},{tokens[5]},{tokens[6]},{tokens[7]},{cantidad}\n")
                f.close()
                for i in activeMachines:
                    f = open(path+i+".csv" ,"a")
                    f.write(f"{tokens[1]},{tokens[2]},{tokens[3]} {tokens[4]},{tokens[5]},{tokens[6]},{tokens[7]},{cantidad}\n")
                print("LOAD PRODUCT SUCCESS")
            except:
                print("LOAD PRODUCT FAILED")

        case "A8":
            general = f"/home/adm-user1/proyecto/distribuidos/clientes/clientes.csv" 
            try:
                f = open(general, "a")
                f.write(f"{tokens[1]},{tokens[2]},{tokens[3]},{tokens[4]},{tokens[5]}\n")
                f.close()
                print("LOAD CLIENT SUCCESS")
            except:
                print("LOAD CLIENT FAILED")

        case "A9":
            pass

        case "FF":
            pass

def activeServer():
    global ipBase
    global port
    global localIP
    global activeMachines
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((ipBase + localIP, port))
    server.listen(5)
    print(f"LISTENING ON {localIP}")
    while True:
        conn, cliente = server.accept()
        print(f"NEW CONNECTION FROM {cliente[0]}")
        tokens = cliente[0].split(".")
        if tokens[-1] not in activeMachines:
            activeMachines.append(tokens[-1])
            print(f"{tokens[-1]} ADD TO ACTIVEMACHINES")
        message = conn.recv(1024).decode('utf-8')
        if message != "":
            print(f"RECEIVEC {message} from {cliente[0]}")
            analizedMessage(message, server)
            response = "FF"
            conn.send(response.encode('utf-8'))
        conn.close()
    server.close()

def sendMessage(ip, message):
    global ipBase
    global port
    global activeMachines
    print(f"SENDING {message} to {ip}")
    try:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.settimeout(5)
        cliente.connect((ipBase + ip, port))
        cliente.send(message.encode('utf-8'))
        if ip not in activeMachines:
            activeMachines.append(ip)
            print(f"{ip} ADD ACTIVE DIRECTORY")
        response = cliente.recv(1024).decode('utf-8')
        analizedMessage(response, cliente)
        print(f"SEND {message} to {ip} SUCESS!!!")
        return True
    except :
        cliente.close()
        print(f"SEND {message} to {ip} FAILED!!!")
        return False

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
                if i not in activeMachines:
                    activeMachines.append(i)
                print(f"{i} ADDED TO ACTIVE MACHINES DIRECTORY")
            except:
                print(f"{i} FAILED CONNECTION")
    print(activeMachines)

def sendAll(*message):
    global activeMachines
    global ipBase
    global masterNode
    inicial = activeMachines

    if(len(message) == 1):
        for i in inicial:
            result = sendMessage(i, message[0])
            if result == False:
                if i == masterNode:
                    masterNode = ""
                activeMachines.remove(i)
                sendAll(f"A1 {i}")
                print(f"{i} REMOVED ACTIVE DIRECTORY")
    else:
        for i in inicial:
            result =  sendMessage(i, message[0] + " "+message[1])
            if result == False:
                if i == masterNode:
                    masterNode = ""
                activeMachines.remove(i)
                sendAll(f"A1 {i}")
                print(f"{i} REMOVED ACTIVE DIRECTORY")          

def sendActive():
    global masterNode
    global localIP
    global activeMachines

    if(threading.active_count() == 3):
        if masterNode != localIP:
            result = sendMessage(masterNode, f"A4 {localIP}")
            if result == False:
                print("Master Node Failed")
                activeMachines.remove(masterNode)
                sendAll(f"A1 {masterNode}")
                masterNode = ""
                if(len(activeMachines) > 1):
                    activeMachines.append(localIP)
                    activeMachines.sort()
                    index = activeMachines.index(localIP)
                    if(index == len(activeMachines) -1):
                        sendAll(f"A2 {localIP}")
                        masterNode = localIP
                        activeMachines.remove(localIP)
                        return
                    else:
                        next = activeMachines[index + 1]
                        activeMachines.remove(localIP)
                        sendMessage(next, f"A3 {localIP}")
                else:
                    if localIP > activeMachines[0]:
                        masterNode = localIP
                    else:
                        masterNode = activeMachines[0]
                    sendAll(f"A2 {masterNode}")
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
    df = pd.read_csv("/home/adm-user1/proyecto/distribuidos/productos/productos.csv")
    if len(activeMachines) >0:
        df["Existencias"] = df["Existencias"] // (len(activeMachines)+1)
        df["Exceso"] = df["Existencias"] % (len(activeMachines)+1)
        pf = df[["ItemID","ItemBarcode", "ItemName", "Price", "Cost", "Categoria", "Existencias"]]

        for i in activeMachines:
            pf.to_csv(f"/home/adm-user1/proyecto/distribuidos/productos/{i}.csv", index=False)
        pf.to_csv(f"/home/adm-user1/proyecto/distribuidos/productos/{localIP}.csv", index=False)
    else:
        df.to_csv(f"/home/adm-user1/proyecto/distribuidos/productos/{localIP}.csv", index=False)
    defineMaster(localIP)
    sendFiles()
    sendAll(f"A2 {localIP}")
    print("ALL FILES SENDING")

def copyFile(ip, file):
    global ipBase
    global port
    global localIP

    send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send.connect((ipBase + ip, port))
    message = f"A5 {file}"
    send.send(message.encode('utf-8'))
    send.close()
    sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sendSocket.connect((ipBase + ip, port))
    path = f"/home/adm-user1/proyecto/distribuidos/productos/{file}"
    with open(path, 'r') as f:
        # Envía el archivo al servidor
        data = f.read(1024)
        while data:
            sendSocket.send(data.encode('utf-8'))
            data = f.read(1024)

    print(f"Archivo {file} enviado con éxito")
    sendSocket.close()

def sendFiles():
    global activeMachines
    global ipBase
    global localIP
    files = os.listdir("/home/adm-user1/proyecto/distribuidos/productos/")

    for i in activeMachines:
        for j in files:
            copyFile(i, j)
    print("ALL FILES SENDED")

def sell(ip, itemid, cantidad, cliente):
    message = f"A6 {ip} {itemid} {cantidad} {cliente}"
    sendAll(message)
    path = f"/home/adm-user1/proyecto/distribuidos/productos/{ip}.csv"
    general = f"/home/adm-user1/proyecto/distribuidos/productos/productos.csv"
    try:
        df = pd.read_csv(path)
        item_id_a_modificar = int(itemid)
        indice_fila = df.index[df['ItemID'] == item_id_a_modificar].tolist()[0]
        linea = df[df['ItemID'] == item_id_a_modificar]
        if df.at[indice_fila, 'Existencias'] >= int(cantidad):
            df.at[indice_fila, 'Existencias'] = df.at[indice_fila, 'Existencias'] - int(cantidad)
        else:
            cantidad = df.at[indice_fila, 'Existencias']
            df.at[indice_fila, 'Existencias'] = df.at[indice_fila, 'Existencias'] - int(cantidad)
        df.to_csv(path, index=False)

        df = pd.read_csv(general)
        item_id_a_modificar = int(itemid)
        indice_fila = df.index[df['ItemID'] == item_id_a_modificar].tolist()[0]
        if df.at[indice_fila, 'Existencias'] >= int(cantidad):
            df.at[indice_fila, 'Existencias'] = df.at[indice_fila, 'Existencias'] - int(cantidad)
        else:
            cantidad = df.at[indice_fila, 'Existencias']
            df.at[indice_fila, 'Existencias'] = df.at[indice_fila, 'Existencias'] - int(cantidad)
        df.to_csv(general, index=False)
        total_costo = float(cantidad) * float(linea['Price'])
        f = open("/home/adm-user1/proyecto/distribuidos/movimientos/movimientos.csv", "a")
        f.write(f"{ip},{itemid},{cliente},{cantidad},{total_costo}\n")
        print("ORDER SELL CORRECT!!")
    except:
        pass

def addProduct(ItemID,ItemBarcode,ItemName,Price,Cost,Categoria,Existencias):
    global activeMachines
    global localIP
    path = f"/home/adm-user1/proyecto/distribuidos/productos/"
    general = f"/home/adm-user1/proyecto/distribuidos/productos/productos.csv" 
    message = f"A7 {ItemID} {ItemBarcode} {ItemName} {Price} {Cost} {Categoria} {Existencias}"
    sendAll(message)
    try:
        cantidad = int(Existencias) // (len(activeMachines) +1)
        f = open(general, "a")
        f.write(f"{ItemID},{ItemBarcode},{ItemName},{Price},{Cost},{Categoria},{Existencias}\n")
        f.close()
        f = open(path + localIP + ".csv", "a")
        f.write(f"{ItemID},{ItemBarcode},{ItemName},{Price},{Cost},{Categoria},{cantidad}\n")
        f.close()
        for i in activeMachines:
            f = open(path+i+".csv" ,"a")
            f.write(f"{ItemID},{ItemBarcode},{ItemName},{Price},{Cost},{Categoria},{cantidad}\n")
        print("LOAD PRODUCT SUCCESS")
    except:
        print("LOAD PRODUCT FAILED")

def addClient(ClienteID,Nombre,Apellido,Email,Telefono):
    global activeMachines
    global localIP
    general = f"/home/adm-user1/proyecto/distribuidos/clientes/clientes.csv" 
    message = f"A8 {ClienteID} {Nombre} {Apellido} {Email} {Telefono}"
    sendAll(message)
    try:
        f = open(general, "a")
        f.write(f"{ClienteID},{Nombre},{Apellido},{Email},{Telefono}\n")
        f.close()
        print("LOAD CLIENT SUCCESS")
    except:
        print("LOAD CLIENT FAILED")

def seeClients():
    path = "/home/adm-user1/proyecto/distribuidos/clientes/clientes.csv"
    df = pd.read_csv(path)
    print(df)

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

    "07": {
        "function": sell,
        "def": "Registra la venta de un producto"
    },

    "08": {
        "function": addProduct,
        "def": "agrega un producto y lo distribuye"
    },

    "09": {
        "function": addClient,
        "def": "agrega un cliente"
    },

    "0A": {
        "function": None,
        "def": "agrega un cliente"
    },

    "0B": {
        "function": None,
        "def": "agrega un cliente"
    },

    "0C": {
        "function": seeClients,
        "def": "agrega un cliente"
    },

    "0D": {
        "function": None,
        "def": "agrega un cliente"
    },

    "0E": {
        "function": None,
        "def": "agrega un cliente"
    },

}

server_t = threading.Thread(target=activeServer)
server_t.start()
print(f"WRITE AN ACCION FOR START")

while True:
    t = input()
    try:
        accion = typeAction[t]["function"]

        match t:
            case "02":
                inp = input("IP,MESSAGE:")
                sp = inp.split(",")
                action_t = threading.Thread(target=accion, args=(sp[0], sp[1]))
                action_t.start()

            case "03":
                inp = input("MESSAGE:")
                action_t = threading.Thread(target=accion, args=[inp])
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
            
            case "07":
                inp = input("Maquina,Producto,Cantidad,Cliente:")
                sp = inp.split(",")
                action_t = threading.Thread(target=accion, args=(sp[0], sp[1], sp[2], sp[3]))
                action_t.start()

            case "08":
                inp = input("ItemID,ItemBarcode,ItemName,Price,Cost,Categoria,Existencias:")
                sp = inp.split(",")
                action_t = threading.Thread(target=accion, args=(sp[0], sp[1], sp[2], sp[3], sp[4], sp[5], sp[6]))
                action_t.start()
            #agregar inventario general

            case "09":
                inp = input("ClienteID,Nombre,Apellido,Email,Telefono:")
                sp = inp.split(",")
                action_t = threading.Thread(target=accion, args=(sp[0], sp[1], sp[2], sp[3], sp[4]))
                action_t.start()
            #agregar cliente

            case "0A":
                print(masterNode)

            case "0B":
                print(activeMachines)

            case "0D":
                path = "/home/adm-user1/proyecto/distribuidos/productos/productos.csv"
                df = pd.read_csv(path)
                print(df)
        
            case "0E":
                inp = input("Sucursal:")
                path = f"/home/adm-user1/proyecto/distribuidos/productos/{inp}.csv"
                df = pd.read_csv(path)
                print(df)

            case _:
                action_t = threading.Thread(target=accion)
                action_t.start()
    except:
        print("INCCORRECT ACTION")
