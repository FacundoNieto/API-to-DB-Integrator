#obtengo el json desde una api y cargo la información en una base de datos
import schedule
import time

import requests
import pymysql
import json

def cargar_datos():
    # Obtener el JSON desde la API
    url = 'http://direccion/api/obtenerpedidos'
    response = requests.get(url)
    json_str = response.text
    json_obj = json.loads(json_str)
    if len(json_obj) == 0:
        print("JSON vacio")

    # Conexión a la base de datos
    conn = pymysql.connect(
        host='',
        user='',
        password='',
        database=''
    )
    cursor = conn.cursor()

    for pedido in json_obj:
        sql_check = "SELECT id_pedido FROM pedidos WHERE id_pedido = %s"
        cursor.execute(sql_check, pedido['id_pedido'])
        result = cursor.fetchone()

        if result is None:
            sql_insert_pedido = """
            INSERT INTO pedidos (id_pedido, id_empresa, estado_pedido, fecha_pedido, _origen_id_sucursal, id_cliente, observaciones)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
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

            if cursor.rowcount > 0:
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

                # Obtener el número de pedido
                num_pedido = pedido['id_pedido']

                # Obtener la cantidad de artículos
                cant_articulos = len(pedido['lin_pedido'])

                # Incrementar nro_proximo en 1
                sql_update_talonarios = "UPDATE talonarios SET nro_proximo = nro_proximo + 1 WHERE nro_caja = 90 AND tipo_talon = 'PED'"
                cursor.execute(sql_update_talonarios)
                conn.commit()

                # Obtener la cantidad de pedidos
                sql_count_pedidos = "SELECT COUNT(*) FROM pedidos"
                cursor.execute(sql_count_pedidos)
                count_pedidos = cursor.fetchone()[0]

                # Imprimir los datos requeridos
                print("Solicitud cargada con éxito.")
                print("Número de pedido:", num_pedido)
                print("Cantidad de artículos:", cant_articulos)
                print("Cantidad de pedidos:", count_pedidos)
        else:
            print("No se ingresaron nuevas solicitudes")

    # Sumar el valor de nro_proximo de la tabla talonarios
    sql_sum_nro_proximo = "SELECT nro_proximo FROM talonarios WHERE nro_caja = 90 AND tipo_talon = 'PED'"
    cursor.execute(sql_sum_nro_proximo)
    sum_nro_proximo = cursor.fetchone()[0]
    print("Suma de nro_proximo:", sum_nro_proximo)

    conn.close()

# Programar la ejecución de la función cada 45 segundos
schedule.every(45).seconds.do(cargar_datos)

while True:
    schedule.run_pending()
    time.sleep(1)
