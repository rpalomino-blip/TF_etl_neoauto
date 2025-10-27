from prefect import task
import mysql.connector

@task
def load(data):
    resultado = 0
    # conexion a bd
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='db_g5'
    )

    #creamos tabla de autos
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS tbl_autos;')
    conn.commit()
    # Crear la tabla si no existe
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tbl_autos (
        id INT AUTO_INCREMENT PRIMARY KEY,
        titulo VARCHAR(255),
        link TEXT,
        etiqueta TEXT,
        imagen TEXT,
        combustible VARCHAR(255),
        ubicacion VARCHAR(255),
        precio DOUBLE,
        marca VARCHAR(255),
        anio VARCHAR(255),
        anunciante VARCHAR(255),
        fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ''')
    conn.commit()
    
    insert_query = """
                insert into tbl_autos(titulo,link,etiqueta,imagen,combustible,ubicacion,precio,marca,anio,anunciante)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
    cursor.executemany(insert_query,data)
    resultado = cursor.rowcount
    conn.commit()
    print('datos importados correctamente')
    cursor.close()
    conn.close()
    
    return resultado
    