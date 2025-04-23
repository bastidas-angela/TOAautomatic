import os
import re
import pandas as pd
import sqlite3
from datetime import timedelta
import shutil

persistencia_antes_remedy=0.5 #media hora
rango_espera=0.25 # 15 minutos

# --- 1. Configuración de rutas y conexión a la base de datos ---
# Obtener el directorio del perfil del usuario actual:
user_profile = os.environ.get("USERPROFILE")
base_path = os.path.join(user_profile, "OneDrive - Telefonica", "Dalia Paola Rodriguez Cruz's files - TOA_proceso")
carpeta_base = "Remedy base"
carpeta_old = os.path.join(base_path, carpeta_base, "old")

if not os.path.exists(carpeta_old):
    os.makedirs(carpeta_old)

conexion = sqlite3.connect(os.path.join(base_path, "tickets_data.db"))
tabla_base = "remedy_base"

# Lista de columnas que usaremos
columnas = [
    "ID_incidencia",
    "Estado",
    "Fecha_envio",
    "Fecha_cierre",
    "Fecha_inicio_incidente",
    "Fecha_fin_incidente",
    "Tipo_afectacion",
    "Resumen",
    "Notas",
    "Grupo_asignado"
]

# --- 2. Leer la tabla remedy_base desde la base de datos o crearla en blanco ---
query_check = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tabla_base}';"
tabla_existe = pd.read_sql(query_check, conexion).shape[0] > 0
if tabla_existe:
    print("📂 Leyendo la tabla base remedy_base desde la base de datos")
    df_remedy_base = pd.read_sql(f"SELECT * FROM {tabla_base}", conexion)
    # Asignamos un valor de orden menor para registros existentes, por ejemplo -1
    df_remedy_base["orden_archivo"] = -1
else:
    print("⚠️ La tabla remedy_base no existe. Se crea un DataFrame vacío.")
    df_remedy_base = pd.DataFrame(columns=columnas + ["orden_archivo"])

# --- 3. Leer los nuevos archivos (orden alfabético) y asignar orden ---
dataframes_nuevos = []  # Guardaremos (nombre_archivo, DataFrame)
archivos = sorted(os.listdir(os.path.join(base_path, carpeta_base)))
orden_inicial = 1  # Para asignar un orden a cada archivo nuevo

for archivo in archivos:
    if (archivo.endswith(".xlsx") and 
        "Remedy_procesado" not in archivo and 
        "alarmas" not in archivo and 
        archivo.lower() != "remedy_base.xlsx"):
        
        ruta_archivo = os.path.join(base_path, carpeta_base, archivo)
        print(f"📂 Procesando archivo: {archivo}")
        
        df_temp = pd.read_excel(ruta_archivo, skiprows=2)
        df_temp = df_temp[[
            "ID de la incidencia*+",
            "Estado*",
            "Fecha de envío",
            "Fecha de cierre",
            "Fecha inicio incidente",
            "Fecha fin incidente",
            "Tipo de Afectación",
            "Resumen*",
            "Notas",
            "Grupo asignado*+"
        ]]
        df_temp.columns = columnas
        # Asignamos el número de orden al archivo que se está procesando.
        df_temp["orden_archivo"] = orden_inicial
        orden_inicial += 1
        
        dataframes_nuevos.append((archivo, df_temp))

# --- 4. Concatenar nuevos datos ---
if dataframes_nuevos:
    df_nuevos = pd.concat([df for _, df in dataframes_nuevos], ignore_index=True)
else:
    df_nuevos = pd.DataFrame(columns=columnas + ["orden_archivo"])

# --- 5. Combinar la tabla base con los nuevos datos ---
df_completo = pd.concat([df_remedy_base, df_nuevos], ignore_index=True)

# Convertir Fecha_inicio_incidente a datetime (por si se requiere en otras comparaciones)
columnas_fecha = ["Fecha_envio", "Fecha_cierre", "Fecha_fin_incidente", "Fecha_inicio_incidente"]

for col in columnas_fecha:
    df_completo[col] = pd.to_datetime(df_completo[col], errors="coerce", dayfirst=True)

# --- 6. Seleccionar la fila a conservar para cada ID_incidencia ---
# Para conservar la fila del documento leído último, ordenamos de forma descendente por "orden_archivo"
df_completo = df_completo.sort_values(by="orden_archivo", ascending=False)
# Luego, eliminamos duplicados; "keep='first'" conserva la fila con mayor valor de orden_archivo
df_final = df_completo.drop_duplicates(subset="ID_incidencia", keep="first")

# Ordenar por Fecha_inicio_incidente
df_final = df_final.sort_values(by="Fecha_inicio_incidente", ascending=True)

# --- 7. Filtrar por Grupo_asignado (FLM COMFICA o FLM HUAWEI) ---
df_final = df_final[df_final["Grupo_asignado"].str.contains("FLM COMFICA|FLM HUAWEI", na=False)]
print(f"📋 Se encontraron {len(df_final)} incidencias con 'FLM' en 'Grupo_asignado'")

df_final = df_final.where(pd.notnull(df_final), None)
# Ejemplo para la columna 'orden_archivo'
df_final["orden_archivo"] = df_final["orden_archivo"].astype(int)

for col in columnas_fecha:
    df_final[col] = df_final[col].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(x) else None)

# --- 8. Guardar la tabla actualizada en la base de datos ---
df_final.to_sql(tabla_base, conexion, if_exists="replace", index=False)
print(f"💾 Tabla actualizada guardada en la base de datos en la tabla '{tabla_base}'")

# --- 9. Mover los archivos procesados a la carpeta "old" ---
for archivo, _ in dataframes_nuevos:
    origen = os.path.join(base_path, carpeta_base, archivo)
    destino = os.path.join(base_path, carpeta_base, archivo)
    shutil.move(origen, destino)
    print(f"📂 Archivo {archivo} movido a la carpeta 'old'.")

df_final["Fecha_inicio_incidente"] = pd.to_datetime(df_final["Fecha_inicio_incidente"], dayfirst=True, errors="coerce")
fecha_inicio_filtro = pd.Timestamp("2024-09-01")
df_final = df_final[df_final["Fecha_inicio_incidente"] >= fecha_inicio_filtro]

print(f"📊 El tamaño de la tabla final es: {len(df_final)}")

df_resultado = df_final.drop(columns=["orden_archivo"], errors="ignore")

print("✅ Ya tenemos la base Remedy lista para procesar ✅")

#####################################################################################################################################################################



# Extraer el valor entre el primer y segundo " | " en la columna "Resumen"
df_resultado["Alarma"] = df_resultado["Resumen"].str.extract(r'(?<=\|)([^|]+)(?=\||$)')
df_resultado["Alarma"] = df_resultado["Alarma"].str.lower()

# Leer el archivo alarmas.xlsx
ruta_alarmas = os.path.join(base_path, carpeta_base, "alarmas.xlsx")
df_alarmas = pd.read_excel(ruta_alarmas)

# Convertir la columna de alarmas a minúsculas para comparación
lista_alarmas = df_alarmas["Alarma"].str.lower().str.strip().tolist()

# Evaluar si el valor de la columna "Alarma" cumple con las condiciones
def evaluar_alarma(alarma):
    if pd.isna(alarma):
        return None
    alarma = alarma.lower()
    if alarma in lista_alarmas or ("ac" in alarma and ("failure" in alarma or "fallo" in alarma or "falla" in alarma)):
        return alarma
    return None

df_resultado["Alarma"] = df_resultado["Alarma"].apply(evaluar_alarma)

print(f"📋 Se encontraron {df_resultado['Alarma'].notna().sum()} incidencias con alarmas no vacías")

# Rellenar los valores vacíos de "Alarma" buscando en "Notas"
df_resultado["Alarma"] = df_resultado["Alarma"].fillna(
    df_resultado["Notas"].str.extract(r'Alarma: (.*?)\n')[0]
).str.lower()
# Eliminar los espacios al inicio y al final de los valores en la columna "Alarma"
df_resultado["Alarma"] = df_resultado["Alarma"].str.strip()

print(f"📋 Se encontraron {df_resultado['Alarma'].notna().sum()} incidencias con alarmas no vacías")

df_resultado["Alarma"] = df_resultado["Alarma"].fillna("").astype(str)

# Hacer merge con df_alarmas usando la columna "Alarma"
df_alarmas["Alarma"] = df_alarmas["Alarma"].str.lower().str.strip()

df_resultado = pd.merge(
    df_resultado,
    df_alarmas,
    left_on="Alarma",
    right_on="Alarma",
    how="left"
)
# Agregar una nueva columna "Tipo" basada en la condición de alarma
df_resultado["Tipo"] = df_resultado.apply(
    lambda row: "TOTAL" if ("ac" in row["Alarma"] and ("failure" in row["Alarma"] or "fallo" in row["Alarma"] or "falla" in row["Alarma"])) else row["Tipo"],
    axis=1
)

# Rellenar los valores vacíos de "Alarma" con "alarma no identificada"
df_resultado["Alarma"] = df_resultado["Alarma"].apply(lambda x: "alarma no identificada" if x == "" else x)

# Rellenar los valores vacíos de "Tipo" y "Alarma" con las condiciones especificadas
df_resultado["Tipo"] = df_resultado.apply(
    lambda row: "alarma no identificada" if (row["Alarma"] == "alarma no identificada") else 
                ("tipo no identificado" if pd.isna(row["Tipo"])  else row["Tipo"]),
    axis=1
)

print("✅ Ya identificamos las alarmas ✅")

#####################################################################################################################################################################



# Extraer la lista de Codigo_Unico de la tabla info_sitios
query_info_sitios = "SELECT Codigo_Unico, Proveedor_FLM FROM info_sitios"
df_info_sitios = pd.read_sql_query(query_info_sitios, conexion)
codigo_unico_list = df_info_sitios["Codigo_Unico"].tolist()

# Extraer todos los patrones que coincidan con el regex
df_resultado["ID_Sitio_All"] = df_resultado["Notas"].str.findall(r'((?!NC|CD|CR)[A-Z]{2}\d{5})').apply(lambda x: list(set(x)))

# Filtrar los valores que están en codigo_unico_list y asignar el primero encontrado a "ID_Sitio"
df_resultado["ID_Sitio"] = df_resultado["ID_Sitio_All"].apply(
    lambda sitios: next((sitio for sitio in sitios if sitio in codigo_unico_list), None)
)

# Clasificar como "Caso Empresa" en la columna Razones_Sin_TOA si se encuentra el patrón "CD+6 dígitos" o la cadena "Circuito:" en el campo "Notas"
df_resultado["Razones_Sin_TOA"] = df_resultado["Notas"].apply(
    lambda notas: "Caso Empresa" if pd.notna(notas) and (bool(re.search(r"CD\d{6}", notas)) or bool(re.search(r"CR\d{5}", notas)) or "Circuito:" in notas or "CIRCUITO:" in notas) else None
)

# Extraer los valores después de "TOA:" o "SIOM:" y antes de un salto de línea en la columna "Notas", ignorando mayúsculas o minúsculas
df_resultado["TOA_notas"] = df_resultado["Notas"].str.extract(r'(?i)(?:TOA:|SIOM:)(.*?)(?:\n|$)')[0].str.strip()
df_resultado["TOA_notas"] = df_resultado["TOA_notas"].str.extract(r'(\d{8})')[0]
df_resultado["TOA_notas"] = df_resultado["TOA_notas"].fillna("sin TOA en notas")

print("✅ Ya identificamos el site id, caso empresa y TOA en notas ✅")

#####################################################################################################################################################################


# Leer la tabla tickets_TOA
query = """
SELECT 
    Nro_TOA, 
    ID_del_Ticket, 
    Número_de_Petición,
    Fecha_de_Registro_de_actividad_TOA,
    Código_de_Cliente, 
    Fecha_Hora_de_Cancelación, 
    Estado_TOA 
FROM tickets_TOA
"""
df_tickets_toa = pd.read_sql_query(query, conexion)

# Eliminar espacios al inicio y al final de las columnas "ID_incidencia" y "ID_del_Ticket"
df_resultado["ID_incidencia"] = df_resultado["ID_incidencia"].str.strip()

df_tickets_toa["ID_del_Ticket"] = df_tickets_toa["ID_del_Ticket"].str.strip()

df_tickets_toa["Número_de_Petición"] = (
    df_tickets_toa["Número_de_Petición"]
    .str.strip()
    .str.replace(r"-\d{2}$", "", regex=True)  # Solo si el patrón está al final de la cadena
)

df_tickets_toa["Clave_Remedy"] = df_tickets_toa["ID_del_Ticket"]
# Si "Clave_Remedy" no coincide con el patrón "INC+7 dígitos", asignar el valor de "Número_de_Petición"
df_tickets_toa["Clave_Remedy"] = df_tickets_toa["Clave_Remedy"].where(
    df_tickets_toa["Clave_Remedy"].str.match(r"INC\d{7}"),
    df_tickets_toa["Número_de_Petición"]
)

def limpiar_nro_toa(valor):
    # Si está vacío o es NaN, devolvemos cadena vacía
    if pd.isna(valor) or valor == "":
        return None
    try:
        # Convertir primero a float (por si viene con ".0"), luego a int, finalmente a str
        entero = int(float(valor))
        # Opcional: si quieres forzar que sean exactamente 8 dígitos, rellena con ceros a la izquierda
        return str(entero).zfill(8)
    except:
        # Si no se puede convertir, retornamos tal cual o un valor distintivo
        return str(valor)

df_tickets_toa["Nro_TOA"] = df_tickets_toa["Nro_TOA"].apply(limpiar_nro_toa)

# Unir df_resultado y df_tickets_toa en base a una clave común
df_unido = pd.merge(
    df_resultado,
    df_tickets_toa,
    left_on="ID_incidencia",  # Cambiar por la columna correspondiente en df_resultado
    right_on="Clave_Remedy",  # Cambiar por la columna correspondiente en df_tickets_toa
    how="left" 
)

print (f"📋 Se encontraron {len(df_unido)} incidencias")

print("✅ Ya cruzamos con la información de TOA ✅")

#####################################################################################################################################################################

# Función para llenar las columnas de TOA cuando Nro_TOA esté vacío,
# utilizando el valor extraído en "TOA_notas" para buscar en df_tickets_toa.
def completar_toa(row):
    # Verificamos que Nro_TOA esté vacío y que TOA_notas contenga un valor válido.
    if (pd.isna(row["Nro_TOA"]) or str(row["Nro_TOA"]).strip() == "" or str(row["Nro_TOA"]).strip() == "<NA>") and row["TOA_notas"] != "sin TOA en notas":
        # Valor candidato extraído de "TOA_notas"
        candidato = row["TOA_notas"].strip()
        # Buscar en df_tickets_toa la fila donde el campo "Nro_TOA" coincide con el candidato.
        coincidencias = df_tickets_toa[df_tickets_toa["Nro_TOA"] == candidato]
        if not coincidencias.empty:
            # Si se encuentra coincidencia, tomar la primera fila encontrada.
            match = coincidencias.iloc[0]
            # Actualizar las columnas con la información obtenida.
            row["Nro_TOA"] = match["Nro_TOA"]
            row["ID_del_Ticket"] = match["ID_del_Ticket"]
            row["Número_de_Petición"] = match["Número_de_Petición"]
            row["Fecha_de_Registro_de_actividad_TOA"] = match["Fecha_de_Registro_de_actividad_TOA"]
            row["Código_de_Cliente"] = match["Código_de_Cliente"]
            row["Fecha_Hora_de_Cancelación"] = match["Fecha_Hora_de_Cancelación"]
            row["Estado_TOA"] = match["Estado_TOA"]
    return row

# Aplicar la función al DataFrame df_unido
df_unido = df_unido.apply(completar_toa, axis=1)

# Actualizar la columna "Razones_Sin_TOA"
df_unido["Razones_Sin_TOA"] = df_unido.apply(
    lambda row: "Si tiene TOA" if pd.notna(row["Nro_TOA"]) and pd.isna(row["Razones_Sin_TOA"]) 
    else ("TOA no identificado" if pd.isna(row["Nro_TOA"]) and pd.isna(row["Razones_Sin_TOA"]) 
          else row["Razones_Sin_TOA"]),
    axis=1
)

print("✅ Ya llenamos con la información de TOA_notas ✅")

#####################################################################################################################################################################

# Si "ID_Sitio" está vacío pero "Código_de_Cliente" no está vacío, asignar el valor de "Código_de_Cliente" a "ID_Sitio"
df_unido["ID_Sitio"] = df_unido.apply(
    lambda row: row["Código_de_Cliente"] if pd.isna(row["ID_Sitio"]) and pd.notna(row["Código_de_Cliente"]) else row["ID_Sitio"],
    axis=1
)

# Hacer merge con df_info_sitios para obtener el Proveedor_FLM
df_unido = pd.merge(
    df_unido,
    df_info_sitios,
    left_on="ID_Sitio",
    right_on="Codigo_Unico",
    how="left"
).drop(columns=["Codigo_Unico"])

# Actualizar la columna "Razones_Sin_TOA" si el Proveedor_FLM no es Huawei o Comfica
df_unido["Razones_Sin_TOA"] = df_unido.apply(
    lambda row: "Sitio corresponde a Telefonica" if row["Proveedor_FLM"] not in ["HUAWEI", "COMFICA"] and (row["Razones_Sin_TOA"] != "Si tiene TOA") and pd.notna(row["ID_Sitio"]) else row["Razones_Sin_TOA"],
    axis=1
)

# Convertir las columnas de fecha a formato datetime
df_tickets_toa["Fecha_de_Registro_de_actividad_TOA"] = pd.to_datetime(df_tickets_toa["Fecha_de_Registro_de_actividad_TOA"], errors="coerce")
df_unido["Fecha_envio"] = pd.to_datetime(df_unido["Fecha_envio"], errors="coerce")

# Lista de columnas donde guardas valores potencialmente no numéricos:
cols_texto = ["Nro_TOA_1", "Remedy_1", "Nro_TOA_2", "Remedy_2"]

for col in cols_texto:
    df_unido[col] = None
    df_unido[col] = df_unido[col].astype(str)


# Iterar sobre las filas donde "Nro_TOA" está vacío pero "ID_Sitio" tiene un valor
for index, row in df_unido[df_unido["Nro_TOA"].isna() & df_unido["ID_Sitio"].notna()].iterrows():
    id_sitio = row["ID_Sitio"]
    fecha_envio = row["Fecha_envio"]

    # Filtrar los tickets en TOA que coincidan con el ID_Sitio y estén dentro del rango de 4 horas post Fecha_envio
    tickets_filtrados = df_tickets_toa[
        (df_tickets_toa["Código_de_Cliente"] == id_sitio) &
        (df_tickets_toa["Fecha_de_Registro_de_actividad_TOA"] <= fecha_envio + timedelta(hours=6)) &
        (df_tickets_toa["Fecha_de_Registro_de_actividad_TOA"] >= fecha_envio - timedelta(hours=6)) &
        (~df_tickets_toa["Nro_TOA"].isin(df_unido["Nro_TOA"].dropna()))
    ]

    # Si se encuentra al menos un ticket, asignar el "Nro_TOA" del primero encontrado
    if not tickets_filtrados.empty:
        if len(tickets_filtrados) >= 1:
            df_unido.at[index, "Nro_TOA_1"] = str(tickets_filtrados.iloc[0]["Nro_TOA"])
            df_unido.at[index, "Remedy_1"]  = str(tickets_filtrados.iloc[0]["Número_de_Petición"])
        else:
            df_unido.at[index, "Nro_TOA_1"] = ""
            df_unido.at[index, "Remedy_1"] = ""

        if len(tickets_filtrados) >= 2:
            df_unido.at[index, "Nro_TOA_2"] = str(tickets_filtrados.iloc[1]["Nro_TOA"])
            df_unido.at[index, "Remedy_2"]  = str(tickets_filtrados.iloc[1]["Número_de_Petición"])
        else:
            df_unido.at[index, "Nro_TOA_2"] = ""
            df_unido.at[index, "Remedy_2"] = ""


print("✅ Ya identificamos posibles TOA ✅")

#####################################################################################################################################################################

# Extraer las columnas Codigo_Unico y priorizacion de la tabla info_sitios
query_info_sitios_extended = "SELECT Codigo_Unico, priorizacion, Tipo_Estacion FROM info_sitios"
df_info_sitios_extended = pd.read_sql_query(query_info_sitios_extended, conexion)

# Unir la información de priorizacion al DataFrame df_unido usando la columna "ID_Sitio"
df_unido = pd.merge(
    df_unido,
    df_info_sitios_extended,
    left_on="ID_Sitio",
    right_on="Codigo_Unico",
    how="left"
).drop(columns=["Codigo_Unico"])

df_unido["priorizacion"] = df_unido["priorizacion"].str.strip()

# Crear la columna "Tiempo de Contención" basada en la columna "priorizacion"
def calcular_tiempo_contencion(priorizacion):
    if priorizacion == "Black":
        return 2
    elif priorizacion == "Oro":
        return 8
    elif priorizacion == "Plata":
        return 10
    elif priorizacion == "Clasico":
        return 10
    else:
        return None

df_unido["Tiempo de Contención"] = df_unido["priorizacion"].apply(calcular_tiempo_contencion)


df_unido["Fecha_de_Registro_de_actividad_TOA"] = pd.to_datetime(df_unido["Fecha_de_Registro_de_actividad_TOA"])
df_unido["Fecha_inicio_incidente"] = pd.to_datetime(df_unido["Fecha_inicio_incidente"])
df_unido["Fecha_fin_incidente"] = pd.to_datetime(df_unido["Fecha_fin_incidente"])


# Crear la columna "Cumplimiento de Contención"
def calcular_cumplimiento_contencion(row):
    if pd.isna(row["Tiempo de Contención"]) or pd.isna(row["Fecha_inicio_incidente"]) or pd.isna(row["Fecha_de_Registro_de_actividad_TOA"]):
        if pd.isna(row["Tiempo de Contención"]):
            return "Sin información Site ID"
        else:
            return "Sin información TOA"
    tiempo_contencion_horas = row["Tiempo de Contención"]
    rango_min = (tiempo_contencion_horas + persistencia_antes_remedy - rango_espera) * 60  # Convertir a minutos
    rango_max = (tiempo_contencion_horas + persistencia_antes_remedy + rango_espera) * 60  # Convertir a minutos
    diferencia_minutos = (row["Fecha_de_Registro_de_actividad_TOA"] - row["Fecha_inicio_incidente"]).total_seconds() / 60  # Diferencia en minutos

    if diferencia_minutos < rango_min:
        return "< del tiempo esperado"
    elif diferencia_minutos > rango_max:
        return "> del tiempo esperado"
    else:
        return "rango correcto"

df_unido["Cumplimiento de Contención"] = df_unido.apply(calcular_cumplimiento_contencion, axis=1)

# Crear la columna "Tiempo de envío" con la diferencia en minutos entre "Fecha_de_Registro_de_actividad_TOA" y "Fecha_inicio_incidente"
df_unido["Tiempo de envío"] = df_unido.apply(
    lambda row: (row["Fecha_de_Registro_de_actividad_TOA"] - row["Fecha_inicio_incidente"]).total_seconds() / 3600 
    if pd.notna(row["Fecha_inicio_incidente"]) and pd.notna(row["Fecha_de_Registro_de_actividad_TOA"]) else None,
    axis=1
)

print("✅ Ya identificamos el cumplimiento de contención ✅")

#####################################################################################################################################################################


# Extraer las columnas ID_TOA, Autin_ID_1, Estado_1, Motivo_Cancel_1 de la tabla consolidada
query_consolidada = "SELECT ID_TOA, Autin_ID_1, Estado_1, Motivo_Cancel_1, Autin_ID_2, Estado_2, Motivo_Cancel_2, Autin_ID_3, Estado_3, Motivo_Cancel_3 FROM tabla_consolidada"
df_consolidada = pd.read_sql_query(query_consolidada, conexion)

# Reemplazar todos los valores NaN con una cadena vacía en el DataFrame df_unido
df_consolidada.fillna("", inplace=True)

# 1. Eliminar duplicados entre Autin_ID_2 y Autin_ID_3
mask_3_duplicado = df_consolidada["Autin_ID_3"] == df_consolidada["Autin_ID_2"]
df_consolidada.loc[mask_3_duplicado, ["Autin_ID_3", "Estado_3", "Motivo_Cancel_3"]] = None

# 2. Eliminar duplicados entre Autin_ID_1 y Autin_ID_2
mask_2_duplicado = df_consolidada["Autin_ID_2"] == df_consolidada["Autin_ID_1"]

# 2.1. Detectar filas con datos en Autin_ID_3
mask_con_3 = df_consolidada["Autin_ID_3"].notna()

# 2.2. Solo en filas donde hay duplicados en ID_1 y ID_2:
# Vaciar ID_2, Estado_2, Motivo_Cancel_2
df_consolidada.loc[mask_2_duplicado, ["Autin_ID_2", "Estado_2", "Motivo_Cancel_2"]] = None

# 2.3. Si hay Autin_ID_3 → moverlo a Autin_ID_2
df_consolidada.loc[mask_2_duplicado & mask_con_3, "Autin_ID_2"] = df_consolidada["Autin_ID_3"]
df_consolidada.loc[mask_2_duplicado & mask_con_3, "Estado_2"] = df_consolidada["Estado_3"]
df_consolidada.loc[mask_2_duplicado & mask_con_3, "Motivo_Cancel_2"] = df_consolidada["Motivo_Cancel_3"]

# 2.4. Limpiar las columnas 3 luego del traspaso
df_consolidada.loc[mask_2_duplicado & mask_con_3, ["Autin_ID_3", "Estado_3", "Motivo_Cancel_3"]] = None

print("✅ Ya identificamos los tickets Autin ✅")

#####################################################################################################################################################################



# Convertir "Nro_TOA" a int, manejando valores vacíos o no convertibles
df_consolidada["ID_TOA"] = df_consolidada["ID_TOA"].astype(str)

# Hacer merge del Nro_TOA y ID_TOA
# Consultar las columnas Task_Id, Complete_Time, Cancel_Time de la tabla tickets_autin
query_autin = "SELECT Task_Id, Complete_Time, Cancel_Time FROM tickets_autin"
df_autin = pd.read_sql_query(query_autin, conexion)

# Hacer merge del Nro_TOA y ID_TOA
df_unido = pd.merge(
    df_unido,
    df_consolidada,
    left_on="Nro_TOA",
    right_on="ID_TOA",
    how="left"
)

# Hacer merge del Task_Id con Autin_ID_1
df_unido = pd.merge(
    df_unido,
    df_autin,
    left_on="Autin_ID_1",
    right_on="Task_Id",
    how="left"
).drop(columns=["Task_Id"])

# Hacer merge del Task_Id con Autin_ID_1
df_unido = pd.merge(
    df_unido,
    df_autin,
    left_on="Autin_ID_2",
    right_on="Task_Id",
    how="left",
    suffixes=("_1", "_2")
).drop(columns=["Task_Id"])

# Hacer merge del Task_Id con Autin_ID_1
df_unido = pd.merge(
    df_unido,
    df_autin,
    left_on="Autin_ID_3",
    right_on="Task_Id",
    how="left"
).drop(columns=["Task_Id"])

# Renombrar las columnas Complete_Time y Cancel_Time a Complete_Time_3 y Cancel_Time_3
df_unido.rename(columns={"Complete_Time": "Complete_Time_3", "Cancel_Time": "Cancel_Time_3"}, inplace=True)

# Convertir las columnas de fecha a formato datetime
df_unido["Cancel_Time_1"] = pd.to_datetime(df_unido["Cancel_Time_1"])
df_unido["Cancel_Time_2"] = pd.to_datetime(df_unido["Cancel_Time_2"])
df_unido["Cancel_Time_3"] = pd.to_datetime(df_unido["Cancel_Time_3"])
df_unido["Fecha_fin_incidente"] = pd.to_datetime(df_unido["Fecha_fin_incidente"])
df_unido["Fecha_Hora_de_Cancelación"] = pd.to_datetime(df_unido["Fecha_Hora_de_Cancelación"])

# Crear la columna "Tiempo de cancelación Autin 1" con la diferencia en horas entre "Cancel_Time_1" y "Fecha_fin_incidente"
df_unido["Tiempo_cancelación_Autin 1"] = df_unido.apply(
    lambda row: (row["Cancel_Time_1"] - row["Fecha_inicio_incidente"]).total_seconds() / 3600
    if pd.notna(row["Cancel_Time_1"]) and pd.notna(row["Fecha_inicio_incidente"]) else None,
    axis=1
)

# Crear la columna "Tiempo de cancelación Autin 2" con la diferencia en horas entre "Cancel_Time_2" y "Fecha_inicio_incidente"
df_unido["Tiempo_cancelación_Autin 2"] = df_unido.apply(
    lambda row: (row["Cancel_Time_2"] - row["Fecha_inicio_incidente"]).total_seconds() / 3600
    if pd.notna(row["Cancel_Time_2"]) and pd.notna(row["Fecha_inicio_incidente"]) else None,
    axis=1
)

# Crear la columna "Tiempo de cancelación Autin 3" con la diferencia en horas entre "Cancel_Time_3" y "Fecha_inicio_incidente"
df_unido["Tiempo_cancelación_Autin 3"] = df_unido.apply(
    lambda row: (row["Cancel_Time_3"] - row["Fecha_inicio_incidente"]).total_seconds() / 3600
    if pd.notna(row["Cancel_Time_3"]) and pd.notna(row["Fecha_inicio_incidente"]) else None,
    axis=1
)

# Crear la columna "Tiempo_cancelación_TOA" con la diferencia en horas entre "Fecha_Hora_de_Cancelación" y "Fecha_inicio_incidente"
df_unido["Tiempo_cancelación_TOA"] = df_unido.apply(
    lambda row: (row["Fecha_Hora_de_Cancelación"] - row["Fecha_inicio_incidente"]).total_seconds() / 3600
    if pd.notna(row["Fecha_Hora_de_Cancelación"]) and pd.notna(row["Fecha_inicio_incidente"]) and row["Estado_TOA"] == "Cancelado" else None,
    axis=1
)

# Crear la columna "Tiempo_cancelación_mínimo" con el valor mínimo entre los tiempos de cancelación calculados
df_unido["Tiempo_cancelación_mínimo"] = df_unido[
    ["Tiempo_cancelación_Autin 1", "Tiempo_cancelación_Autin 2", "Tiempo_cancelación_Autin 3", "Tiempo_cancelación_TOA"]
].apply(lambda row: row[row < 24*4].min(), axis=1)



# Crear la columna "Cumplimiento de Contención"
def error_contencion(row):
    if pd.isna(row["Tiempo de Contención"]) or pd.isna(row["Tiempo_cancelación_mínimo"]):
        if pd.isna(row["Tiempo de Contención"]):
            return "Sin información Site ID"
        elif pd.isna(row["Tiempo_cancelación_mínimo"]):
            if row[["Tiempo_cancelación_Autin 1", "Tiempo_cancelación_Autin 2", "Tiempo_cancelación_Autin 3", "Tiempo_cancelación_TOA"]].isna().all():
                return "Ticket no cancelado"
            else:
                return "Cancelamiento Outlier"
        else:
            return None
    tiempo_contencion_horas = row["Tiempo de Contención"]
    rango_min = (tiempo_contencion_horas + persistencia_antes_remedy - rango_espera) * 60  # Convertir a minutos
    rango_max = (tiempo_contencion_horas + persistencia_antes_remedy + rango_espera) * 60  # Convertir a minutos
    diferencia_minutos = (row["Tiempo_cancelación_mínimo"] ) * 60  # Diferencia en minutos

    if diferencia_minutos < rango_min:
        return "Cancelado antes de rango contención"
    elif diferencia_minutos > rango_max:
        return "Cancelado fuera de rango contención"
    else:
        return "Cancelado en rango contención"

df_unido["Error Contención"] = df_unido.apply(error_contencion, axis=1)

print("✅ Ya se encontraron errores en la contención ✅")

#####################################################################################################################################################################



# Crear la columna "rango de cancelación" basada en el valor de "Tiempo_cancelación_mínimo"
def calcular_rango_cancelacion(row):
    if row["Error Contención"] == "Cancelamiento Outlier":
        return "Cancelamiento Outlier"
    tiempo = row["Tiempo_cancelación_mínimo"]
    if pd.isna(tiempo):
        return None
    elif 0 <= tiempo < 6:
        return "00-06"
    elif 6 <= tiempo < 12:
        return "06-12"
    elif 12 <= tiempo < 18:
        return "12-18"
    elif 18 <= tiempo < 24:
        return "18-24"
    elif 24 <= tiempo < 36:
        return "24-36"
    elif 36 <= tiempo < 48:
        return "36-48"
    elif 48 <= tiempo < 60:
        return "48-60"
    elif 60 <= tiempo < 72:
        return "60-72"
    else:
        return "72+"

df_unido["rango de cancelación"] = df_unido.apply(calcular_rango_cancelacion, axis=1)

print("✅ Ya identificamos el rango de cancelación ✅")

#####################################################################################################################################################################


# Consultar las columnas Codigo_Unico y Fecha_Fin_Swap de la tabla info_sitios
query_info_sitios_swap = "SELECT Codigo_Unico, Fecha_Fin_Swap FROM info_sitios"
df_info_sitios_swap = pd.read_sql_query(query_info_sitios_swap, conexion)

# Convertir Fecha_Fin_Swap a formato datetime
df_info_sitios_swap["Fecha_Fin_Swap"] = pd.to_datetime(df_info_sitios_swap["Fecha_Fin_Swap"], errors="coerce")

# Unir la información de Fecha_Fin_Swap al DataFrame df_unido usando la columna "ID_Sitio"
df_unido = pd.merge(
    df_unido,
    df_info_sitios_swap,
    left_on="ID_Sitio",
    right_on="Codigo_Unico",
    how="left"
).drop(columns=["Codigo_Unico"])

# Clasificar los tickets como "Incidente post SWAP" o "Incidente no relacionado a SWAP"
df_unido["Clasificación SWAP"] = df_unido.apply(
    lambda row: "Sin info de Sitio" if pd.isna(row["ID_Sitio"]) 
    else ("Incidente post SWAP" if pd.notna(row["Fecha_Fin_Swap"]) and row["Fecha_inicio_incidente"] > row["Fecha_Fin_Swap"] 
          else "Incidente no relacionado a SWAP"),
    axis=1
)

print("✅ Ya clasificamos por SWAP ✅")

#####################################################################################################################################################################

query_autin = f"SELECT * FROM tickets_autin"
df_autin_query = pd.read_sql_query(query_autin, conexion)

df_autin_query['Createtime'] = pd.to_datetime(df_autin_query['Createtime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
df_autin = df_autin_query[['Task_Id', 'Task_Category', 'Createtime', 'Task_Status', 'Site_Id']]

# 3. Comprobar duplicados en 'Task_Id'
if df_autin['Task_Id'].duplicated().any():
    print("Hay Task Id duplicados en el dataframe de Autin")

# 4. Filtrar tickets de "Abastecimiento" (no cancelados) y renombrar columnas
Autin_abastecimiento = df_autin[
    (df_autin['Task_Category'].str.contains("Abastecimiento", case=False, na=False)) &
    (df_autin['Task_Status'] != "canceled")
][['Site_Id', 'Task_Id', 'Task_Status', 'Createtime']]
Autin_abastecimiento.columns = ['Site_Id', 'Task_Id_Abastecimiento', 'Task_Status_Abastecimiento', 'Createtime_Abastecimiento']
Autin_abastecimiento.sort_values(by=['Site_Id', 'Createtime_Abastecimiento'], inplace=True)

# Buscar la cantidad de tickets y la lista de Task_Id de Autin Abastecimiento para cada Site_Id en df_unido
def buscar_tickets_abastecimiento(row):
    if pd.isna(row["ID_Sitio"]) or pd.isna(row["Fecha_de_Registro_de_actividad_TOA"]):
        return pd.Series([None, None])
    # Filtrar los registros de Autin Abastecimiento para el mismo Site_Id
    abastecimiento_filtrado = Autin_abastecimiento[
        (Autin_abastecimiento["Site_Id"] == row["ID_Sitio"]) &
        (Autin_abastecimiento["Createtime_Abastecimiento"] <= row["Fecha_de_Registro_de_actividad_TOA"] + timedelta(hours=48)) &
        (Autin_abastecimiento["Createtime_Abastecimiento"] >= row["Fecha_de_Registro_de_actividad_TOA"])
    ]
    # Si hay registros, devolver la cantidad y la lista de Task_Id
    if not abastecimiento_filtrado.empty:
        return pd.Series([len(abastecimiento_filtrado), list(abastecimiento_filtrado["Task_Id_Abastecimiento"])])
    return pd.Series([0, []])

# Aplicar la función al DataFrame df_unido
df_unido[["Cantidad_Tickets_Abastecimiento", "Lista_Abastecimiento"]] = df_unido.apply(buscar_tickets_abastecimiento, axis=1)

# Crear la columna "¿Hubo Abastecimiento?" basada en la cantidad de tickets de abastecimiento
df_unido["¿Hubo Abastecimiento?"] = df_unido["Cantidad_Tickets_Abastecimiento"].apply(
    lambda x: "Si" if pd.notna(x) and x != 0 else "No"
)

print("✅ Ya identificamos los tickets con abastecimiento ✅")

#####################################################################################################################################################################

# Filtrar las columnas deseadas de df_autin_query
columnas_deseadas = [
    'Task_Id', 
    "Arrive_Time",
    "Com_Fault_Speciality",
    "Com_Fault_Sub_Speciality",
    "Com_Fault_Cause",
    "Leave_Observations",
    "Detalle_de_actuación_realizada"
]
df_autin_query_filtrado = df_autin_query[df_autin_query["Task_Id"].str.contains("CM", na=False)][columnas_deseadas]

# Unir las columnas filtradas al DataFrame df_unido usando Task_Id como clave para Autin_ID_1
df_unido = pd.merge(
    df_unido,
    df_autin_query_filtrado.add_suffix("_1"),
    left_on="Autin_ID_1",
    right_on="Task_Id_1",  # Asegurarse de que Task_Id_1 sea la columna con sufijo en df_autin_query_filtrado
    how="left"
).drop(columns=["Task_Id_1"])

# Unir las columnas filtradas al DataFrame df_unido usando Task_Id como clave para Autin_ID_2
df_unido = pd.merge(
    df_unido,
    df_autin_query_filtrado.add_suffix("_2"),
    left_on="Autin_ID_2",
    right_on="Task_Id_2",  # Asegurarse de que Task_Id_2 sea la columna con sufijo en df_autin_query_filtrado
    how="left"
).drop(columns=["Task_Id_2"])

# Unir las columnas filtradas al DataFrame df_unido usando Task_Id como clave para Autin_ID_3
df_unido = pd.merge(
    df_unido,
    df_autin_query_filtrado.add_suffix("_3"),
    left_on="Autin_ID_3",
    right_on="Task_Id_3",  # Asegurarse de que Task_Id_3 sea la columna con sufijo en df_autin_query_filtrado
    how="left"
).drop(columns=["Task_Id_3"])

print("✅ Se han añadido las columnas seleccionadas de df_autin_query ✅")

#####################################################################################################################################################################

# Crear la columna "¿el técnico llego al lugar?" basada en los valores de "Arrive_Time"
df_unido["¿El técnico llego al lugar?"] = df_unido.apply(
    lambda row: "Si" if pd.notna(row["Arrive_Time_1"]) or pd.notna(row["Arrive_Time_2"]) or pd.notna(row["Arrive_Time_3"]) else "No",
    axis=1
)

# Crear la columna "¿Relacionado con Fallo AC?" basada en las condiciones especificadas
df_unido["¿Relacionado con Fallo AC?"] = df_unido.apply(
    lambda row: "Si" if row["Com_Fault_Speciality_1"] == "ENERGIA" and "AC" in str(row["Com_Fault_Sub_Speciality_1"]).upper() else
                ("Si" if row["Com_Fault_Speciality_2"] == "ENERGIA" and "AC" in str(row["Com_Fault_Sub_Speciality_2"]).upper() else
                 ("Si" if row["Com_Fault_Speciality_3"] == "ENERGIA" and "AC" in str(row["Com_Fault_Sub_Speciality_3"]).upper() else "No")),
    axis=1
)

# Función para detectar acción en grupo electrógeno (GE) sin spaCy
def detectar_accion_ge(texto):
    if not texto or not isinstance(texto, str):
        return "NO"
    
    # Convertir a minúsculas para una comparación case-insensitive
    texto = texto.lower()

    # 1. Descarta expresiones negativas que indiquen la ausencia de GE.
    neg_pattern = r"\b(?:no\s+(?:tiene|hay|existe)|sin|ningún(?:a)?|no\s*cuenta\s+con)\s+(?:grupo\s+electr[oó]geno|grupo|ge|g\.e\.?)\b"
    if re.search(neg_pattern, texto):
        return "NO"
    
    # 2. Verificar la existencia de un verbo de acción.
    # Utilizamos raíces genéricas para capturar varias conjugaciones.
    accion_pattern = r"\b(?:instal|encend|cambi|coloc|dej(?:a|o)|oper|funcion)\w*\b"
    if not re.search(accion_pattern, texto):
        return "NO"
    
    # 3. Verificar la mención de grupo electrógeno.
    # Opción A: Mención explícita de "grupo electrógeno"
    pattern_ge_exp = r"\bgrupo\s+electr[oó]geno\b"
    # Opción B: Abreviaturas: "ge", "g.e", "g.e."  
    pattern_ge_abbr = r"\b(?:ge|g\.e\.?)\b"
    # Opción C: La palabra "grupo" sola.
    pattern_grupo = r"\bgrupo\b"
    
    if re.search(pattern_ge_exp, texto) or re.search(pattern_ge_abbr, texto) or re.search(pattern_grupo, texto):
        return "SI"
    
    return "NO"

# Función para detectar acción en baterías sin spaCy
def detectar_accion_baterias(texto):
    if not texto or not isinstance(texto, str):
        return "NO"
    texto = texto.lower()
    
    # Verificar si se encuentra algún verbo de acción asociado a baterías
    accion = re.search(r"\b(?:coloc|cambi|instal|mid|recarg|carg|respald|revis|verific)\w*\b", texto)
    if not accion:
        return "NO"
    
    # Verificar la mención de términos relacionados con "batería"
    menc_bateria = re.search(r"\b(?:b{1,2}[aá]ter[ií]a(?:s)?)\b", texto)
    if menc_bateria:
        return "SI"
    return "NO"

def detectar_accion_itm(texto):
    if not texto or not isinstance(texto, str):
        return "NO"
    
    texto = texto.lower()
    
    # Verificar si se encuentra algún verbo de acción relacionado con cambios o ajustes.
    # Las raíces aquí incluyen: cambi, ajust, reajust, reposicion (capturando sus variantes)
    accion_pattern = r"\b(?:cambi|ajust|reajust|reposicion)\w*\b"
    if not re.search(accion_pattern, texto):
        return "NO"
    
    # Verificar que se haga referencia a ITM
    # Usamos \b para detectar "itm" como palabra completa.
    itm_pattern = r"\bitm\b"
    if re.search(itm_pattern, texto):
        return "SI"
    else:
        return "NO"
    

def detectar_accion_breakers(texto):
    if not texto or not isinstance(texto, str):
        return "NO"
    
    texto = texto.lower()
    
    # Verificar si se encuentra un verbo de acción relacionado
    # Las raíces consideradas: sub[ií]o, levant(o|a), ajust(o|ó), arregl(o|ó)
    accion_pattern = r"\b(?:sub[ií]o|levant(?:o|a)|ajust(?:o|ó)|activ|arregl(?:o|ó))\w*\b"
    if not re.search(accion_pattern, texto):
        return "NO"
    
    # Verificar la mención de breakers.
    breaker_pattern = r"\b(?:breaker|breacker|breackers|braker|bracker|brackers|breker|breckers|braker|brackers|brackers)\b"
    if re.search(breaker_pattern, texto):
        return "SI"
    
    return "NO"

# Combinar los campos en un único texto para el análisis.
# Puedes ajustar este concatenado según tus necesidades.
df_unido["Texto_Comb"] = (
    df_unido["Leave_Observations_1"].fillna("") + ". " +
    df_unido["Detalle_de_actuación_realizada_1"].fillna("") + ". " +
    df_unido["Leave_Observations_2"].fillna("") + ". " +
    df_unido["Detalle_de_actuación_realizada_2"].fillna("") + ". " +
    df_unido["Leave_Observations_3"].fillna("") + ". " +
    df_unido["Detalle_de_actuación_realizada_3"].fillna("")
)

# Aplicar las funciones para crear las nuevas columnas con respuestas "SI" o "NO"
df_unido["¿Hubo acción en el GE?"] = df_unido["Texto_Comb"].apply(detectar_accion_ge)
df_unido["¿Hubo acción en las baterías?"] = df_unido["Texto_Comb"].apply(detectar_accion_baterias)
df_unido["¿Hubo acción en el ITM?"] = df_unido["Texto_Comb"].apply(detectar_accion_itm)
df_unido["¿Hubo acción en los breakers?"] = df_unido["Texto_Comb"].apply(detectar_accion_breakers)

# Crear la columna "Detectamos atención" basada en las columnas de preguntas
df_unido["Detectamos atención"] = df_unido.apply(
    lambda row: "Si" if any([
        row["¿Hubo Abastecimiento?"] == "Si",
        row["¿El técnico llego al lugar?"] == "Si",
        row["¿Relacionado con Fallo AC?"] == "Si",
        row["¿Hubo acción en el GE?"] == "SI",
        row["¿Hubo acción en las baterías?"] == "SI",
        row["¿Hubo acción en el ITM?"] == "SI",
        row["¿Hubo acción en los breakers?"] == "SI"
    ]) else "No",
    axis=1
)

# Guardamos en un excel
df_unido.to_excel(os.path.join(base_path, carpeta_base, "Remedy_procesado.xlsx"), index=False)

# Cerrar la conexión
conexion.close()