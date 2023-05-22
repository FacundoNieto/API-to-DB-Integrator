#obtengo el json desde una api y cargo la información en una base de datos
import schedule
import time

import requests
import pymysql
import json

def cargar_datos():
    # Obtener el JSON desde la API
    url = 'http://111.222.33.444/api/obtenerpedidos'
    response = requests.get(url)
    json_str = response.text
    json_obj = json.loads(json_str)

    # Conexión a la base de datos
    conn = pymysql.connect(
        host='192.168.1.999',
        user='user',
        password='pass',
        database='db_example'
    )
    cursor = conn.cursor()

    for pedido in json_obj:
        sql_check = "SELECT id_pedido FROM pedidos WHERE id_pedido = %s"
        cursor.execute(sql_check, pedido['id_pedido'])
        result = cursor.fetchone()
        print(result)  # Solo para verificar en consola

        if result is None:
            sql_insert_pedido = """
            INSERT INTO pedidos (id_pedido, id_empresa, fecha_pedido, _origen_id_sucursal, id_cliente, observaciones)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            values_pedido = (
                pedido['id_pedido'],
                pedido['id_empresa'],
                pedido['fecha_pedido'],
                pedido['_origen_id_sucursal'],
                pedido['id_cliente'],
                pedido['observaciones']
            )
            cursor.execute(sql_insert_pedido, values_pedido)
            conn.commit()
            conn.rollback()

            if cursor.rowcount > 0:
                print("Agregado correctamente")
                sql_insert_lin_pedido = """
                INSERT INTO lin_pedido (
                    id_pedido, item, id_articulo, cantidad, des_articulo,
                    presentacion, pcio_vta_uni_siva, pcio_com_uni_siva
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                for lin_pedido in pedido['lin_pedido']:
                    values_lin_pedido = (
                        lin_pedido['id_pedido'],
                        lin_pedido['item'],
                        lin_pedido['id_articulo'],
                        lin_pedido['cantidad'],
                        lin_pedido['des_articulo'],
                        lin_pedido['presentacion'],
                        lin_pedido['pcio_vta_uni_siva'],
                        lin_pedido['pcio_com_uni_siva']
                    )
                    cursor.execute(sql_insert_lin_pedido, values_lin_pedido)
                    conn.commit()
                    conn.rollback()
    
    if len(json_obj) == 0:
        print("No hay solicitudes entrantes")
    conn.close()

# Programar la ejecución de la función cada 45 segundos
schedule.every(45).seconds.do(cargar_datos)

while True:
    schedule.run_pending()
    time.sleep(1)
