import sqlite3
import funciones as fn  # Importa el módulo de funciones con toda la lógica de procesamiento
import pandas as pd
import traceback
import time
import os

def procesar_datos():
    """
    Función principal para procesar los datos:
      1. Define rutas y parámetros de origen (archivos y base de datos).
      2. Abre la conexión a la base de datos.
      3. Procesa los archivos de las distintas fuentes (TOA, Autin, Autin PR y SITIOS).
      4. Combina los datos de las tablas en una tabla consolidada.
      5. Exporta el resultado final a un archivo Excel.
      6. Muestra estadísticas de las tablas en la base de datos y el tiempo de ejecución total.
    """
    # Registrar el tiempo de inicio para medir la duración del proceso
    start_time = time.time()

    # Definir la ruta principal donde se encuentran los archivos en OneDrive
    # Obtener el directorio del perfil del usuario actual:
    user_profile = os.environ.get("USERPROFILE")
    base_path = os.path.join(user_profile, "OneDrive - Telefonica", "Proceso TOA")
    print(f"Ruta base: {base_path}")
    
    # Verificar si se tienen permisos de lectura y escritura en la carpeta base
    if not os.access(base_path, os.R_OK):
        raise PermissionError(f"No se tienen permisos de lectura en la carpeta: {base_path}")
    if not os.access(base_path, os.W_OK):
        raise PermissionError(f"No se tienen permisos de escritura en la carpeta: {base_path}")
    
    # Definir las rutas de origen para cada tipo de archivo
    carpeta_origen_TOA = os.path.join(base_path, "TOA base")
    carpeta_origen_autin = os.path.join(base_path, "Autin base", "Autin Tickets")
    carpeta_origen_autin_pr = os.path.join(base_path, "Autin base", "Autin PR")
    carpeta_origen_sitios = os.path.join(base_path, "DATA", "SITIOS")
    
    # Ruta de la base de datos (archivo SQLite) ubicado en OneDrive
    base_datos = os.path.join(base_path, "tickets_data.db")

    # Definir los nombres de las tablas a utilizar en la base de datos
    tabla_TOA = "tickets_TOA"
    tabla_autin = "tickets_autin"
    tabla_autin_pr = "tickets_pr"
    tabla_sitios = "info_sitios"
    tabla_final = "tabla_consolidada"

    # Abrir la conexión a la base de datos (se reutiliza durante todo el proceso)
    conexion = sqlite3.connect(base_datos)
    
    # Eliminar la tabla consolidada si existe para reiniciar el proceso
    cursor = conexion.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {tabla_final}")
    conexion.commit()
    
    try:
        # ============================================================
        # 🔹 1️⃣ Limpiar información de las arpetas Old
        fn.procesar_old()
        print("Proceso completado exitosamente OLD.")

        # ============================================================
        # 🔹 2️⃣ Procesar los archivos descargados de las diferentes fuentes
        print("\nProcesando archivos...\n")
        
        # Procesa los archivos de TOA y actualiza la tabla correspondiente
        fn.procesar_archivos_tickets(carpeta_origen_TOA, tabla_TOA, conexion, 'Nro_TOA')
        print("Proceso completado exitosamente TOA.\n")
        
        # Procesa los archivos de Autin y actualiza la tabla correspondiente
        fn.procesar_archivos_tickets(carpeta_origen_autin, tabla_autin, conexion, 'Task_Id')
        print("Proceso completado exitosamente AUTIN.\n")
        
        # Procesa los archivos de Autin PR y actualiza la tabla correspondiente
        fn.procesar_archivos_tickets(carpeta_origen_autin_pr, tabla_autin_pr, conexion, 'Index')
        print("Proceso completado exitosamente PR.\n")
        
        # Combina los datos de los archivos de SITIOS y actualiza la tabla correspondiente
        fn.combinar_datos_sitios(carpeta_origen_sitios, tabla_sitios, conexion, 'Codigo_Unico')
        print("Proceso completado exitosamente SITIOS.\n")

        # ============================================================
        # 🔹 3️⃣ Combinar tablas y generar el reporte final consolidado
        fn.combinar_tablas(conexion, tabla_TOA, tabla_autin, tabla_sitios, tabla_final)
        print("Proceso completado exitosamente ANALISIS COMPLETO.")

        print("\nTiempo de ejecución sin excel: %s segundos\n" % (time.time() - start_time))

        # ============================================================
        # 🔹 4️⃣ Exportar el resultado final a Excel

        # (Opcional) Guardar todas las tablas en un solo archivo Excel con hojas separadas
        archivo_salida = os.path.join(base_path, "Reporte.xlsx")
        # fn.guardar_todas_las_tablas(conexion, archivo_salida)
        # print(f"\nTodas las tablas han sido guardadas en '{archivo_salida}' correctamente.")

        # Se puede generar un nombre de archivo con marca de tiempo (en este caso se usa una ruta fija)
        hora = time.strftime("%Y%m%d-%H%M%S")
        # archivo = 'ArchivoFinal_' + hora + '.xlsx'
        archivo = os.path.join(base_path, "ArchivoFinal.xlsx")
        
        # Convertir la tabla consolidada a un archivo Excel formateado
        fn.convertir_tabla_a_excel(tabla_final, archivo, conexion, hoja_nombre='Sheet1')
        
    except Exception as e:
        # En caso de error, se muestra el error y la traza completa
        print(f"Error durante la actualización de la base de datos: {e}")
        traceback.print_exc()
    finally:
        # Mostrar un resumen de las tablas existentes y sus tamaños en la base de datos
        cursor = conexion.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = cursor.fetchall()
        print("\nTablas en la base de datos:")

        for tabla in tablas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabla[0]}")
            tamaño = cursor.fetchone()[0]
            print(f"\tTabla: {tabla[0]}, Tamaño: {tamaño}")
        
        # Cerrar la conexión a la base de datos
        conexion.close()

        print("\nTiempo de ejecución: %s segundos" % (time.time() - start_time))


# Ejecutar la función principal
procesar_datos()

