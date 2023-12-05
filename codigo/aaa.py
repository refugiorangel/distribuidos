def initialDistribution():
    global activeMachines  
    global localIP  
    df = pd.read_csv("/home/adm-user1/proyecto/distribuidos/data/PRODUCTOS.csv")
    if len(activeMachines) >0:
        df["Existencias"] = df["Existencias"] // len(activeMachines)+1
        df["Exceso"] = df["Existencias"] % len(activeMachines)+1
        pf = df[["ItemID","ItemBarcode", "ItemName", "Price", "Cost", "Categoria", "Existencias"]]

        for i in activeMachines:
            pf.to_csv(f"/home/adm-user1/proyecto/distribuidos/data/{i}.csv", index=False)
        pf.to_csv(f"/home/adm-user1/proyecto/distribuidos/data/{localIP}.csv")
    else:
        df.to_csv(f"/home/adm-user1/proyecto/distribuidos/data/{localIP}.csv", index=False)


"04": {
    "function": initialDistribution,
    "def": "Manda un mensaje a cualquier ip"
}

