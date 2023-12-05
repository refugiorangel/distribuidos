import paramiko


def copiar_archivos_scp(ip, file):
    global ipBase
    global localIP
    try:
        # Establecer conexión SSH con el servidor de origen
        ssh_origen = paramiko.SSHClient()
        ssh_origen.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_origen.connect(hostname=ipBase+localIP, username="adm-user1", password="contrasenia1")

        # Crear cliente SCP para copiar archivos desde el servidor de origen
        scp_origen = paramiko.SFTPClient.from_transport(ssh_origen.get_transport())

        # Establecer conexión SSH con el servidor de destino
        ssh_destino = paramiko.SSHClient()
        ssh_destino.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_destino.connect(hostname=ipBase+ip, username="adm-user1", password="contrasenia1")

        # Crear cliente SCP para copiar archivos al servidor de destino
        scp_destino = paramiko.SFTPClient.from_transport(ssh_destino.get_transport())

        # Copiar archivos desde el origen al destino
        scp_origen.put(f"/home/adm-user1/proyecto/data/{file}.csv", f"/home/adm-user1/proyecto/data/{file}.csv")

        print(f"Archivos copiados de {localIP}:{file} a {ip}:{file}")

    except Exception as e:
        print(f"Error al copiar archivos: {e}")

    finally:
        # Cerrar conexiones SSH
        ssh_origen.close()
        ssh_destino.close()



copiar_archivos_scp("31", "30")