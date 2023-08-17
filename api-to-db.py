import schedule
import time
import requests
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def cargar_datos():
    # Obtener el JSON desde la API
    url = os.getenv("URL_API")
    response = requests.get(url)
    json_obj = response.json()

    if not json_obj:
        print("JSON vacío")
        return

    # Conexión a la base de datos
    conn = pymysql.connect(
        host= os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        database=os.getenv("DATABASE")
    )
    cursor = conn.cursor()

    for pedido in json_obj:
        sql_check = "SELECT id_pedido FROM pedidos WHERE id_pedido = %s"
        cursor.execute(sql_check, pedido['id_pedido'])
        result = cursor.fetchone() # Fetch the next row (sólo el primer resultado)

        if result is None:
            nuevas_solicitudes = True  # Se encontró una nueva solicitud

            # Reviso si existe nroAfiliado en el pedido, y consulto qué id_persona le corresponde en la tabla personas
            if 'nroAfiliado' in pedido and pedido['nroAfiliado'] is not None:
                sql_id_persona = "SELECT id_persona FROM personas WHERE nombre_secundario = %s"
                cursor.execute(sql_id_persona, str(pedido['nroAfiliado']))
                # devuelve una tupla de un sólo elemento, pero no hace falta extraerlo haciendo id_persona[0]
                id_persona = cursor.fetchone()
                if id_persona is None:
                    print(f"id_persona es: {id_persona}, se buscó pero no se encontró id_persona")
                else:
                    print(f"id_persona encontrado: {id_persona}")
            else:
                id_persona = None
                print(f"id_persona es: {id_persona}, pedido['nroAfiliado'] era 'null' ó no estaba en el json")

            #Ahora sí inserto los datos en las tablas
            sql_insert_pedido = """
            INSERT INTO pedidos (id_pedido, id_empresa, estado_pedido, fecha_pedido, _origen_id_sucursal, id_cliente, observaciones, _destinatario, _intermediario)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values_pedido = (
                pedido['id_pedido'],
                pedido['id_empresa'],
                pedido['estado_pedido'],
                pedido['fecha_pedido'],
                pedido['_origen_id_sucursal'],
                pedido['id_cliente'],
                pedido['observaciones'],
                id_persona, # en "_destinatario"
                10000455 #id_cliente de Apos, se carga en "_intermediario"                
            )
            cursor.execute(sql_insert_pedido, values_pedido)
            conn.commit()

            # si se cargó un registro en la tabla pedidos, cargo en lin_pedido
            if cursor.rowcount > 0:
                
                sql_insert_lin_pedido = """
                INSERT INTO lin_pedido (
                    id_pedido, item, id_articulo, cantidad, des_articulo,
                    presentacion, pcio_vta_uni_siva, pcio_com_uni_siva
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                values_lin_pedido = [
                    (
                        lin_pedido['id_pedido'],
                        #lin_pedido['item'], #viene mal de la api en algunos casos
                        index + 1,
                        lin_pedido['id_articulo'],
                        lin_pedido['cantidad'],
                        lin_pedido['des_articulo'],
                        lin_pedido['presentacion'],
                        lin_pedido['pcio_vta_uni_siva'],
                        lin_pedido['pcio_com_uni_siva']
                    )
                    #for lin_pedido in pedido['lin_pedido']
                    for index, lin_pedido in enumerate(pedido['lin_pedido'])  # Usamos enumerate() para obtener el índice
                ]
                cursor.executemany(sql_insert_lin_pedido, values_lin_pedido)
                conn.commit()

                # Obtener el número de pedido
                num_pedido = pedido['id_pedido']

                # Obtener la cantidad de artículos
                cant_articulos = len(pedido['lin_pedido'])

                # Incrementar nro_proximo en 1
                sql_update_talonarios = "UPDATE talonarios SET nro_proximo = nro_proximo + 1 WHERE nro_caja = 7 AND tipo_talon = 'PED'"
                cursor.execute(sql_update_talonarios)
                conn.commit()

                # Obtener la cantidad de pedidos
                sql_count_pedidos = "SELECT COUNT(*) FROM pedidos"
                cursor.execute(sql_count_pedidos)
                count_pedidos = cursor.fetchone()[0] 

                # Imprimir los datos requeridos
                if nuevas_solicitudes:
                    print("Solicitud cargada con éxito.")
                    print("Número de pedido:", num_pedido)
                    print("Cantidad de artículos:", cant_articulos)
                    print("Cantidad de pedidos:", count_pedidos)

        else:
            nuevas_solicitudes = False
            
    if not nuevas_solicitudes:
        print("No se ingresaron nuevas solicitudes")

    # Sumar el valor de nro_proximo de la tabla talonarios
    sql_sum_nro_proximo = "SELECT nro_proximo FROM talonarios WHERE nro_caja = 7 AND tipo_talon = 'PED'"
    cursor.execute(sql_sum_nro_proximo)
    sum_nro_proximo = cursor.fetchone()[0]
    print("Suma de nro_proximo:", sum_nro_proximo)
    
    hora = time.time()
    hora_legible = time.strftime('%H:%M:%S %d-%m-%Y', time.localtime(hora))
    print(f"Hora: {hora_legible}\n")

    conn.close()

cargar_datos()
# Programar la ejecución de la función cada 45 segundos
schedule.every(45).seconds.do(cargar_datos)

while True:
    schedule.run_pending()
    time.sleep(1)