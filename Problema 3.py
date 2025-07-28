import os
import zipfile
import pandas as pd
import requests
from pymongo import MongoClient


url = 'https://netsg.cs.sfu.ca/youtubedata/0333.zip'
zip_path = '0333.zip'

if not os.path.exists(zip_path):
    print("Descargando archivo ZIP...")
    response = requests.get(url)
    with open(zip_path, 'wb') as f:
        f.write(response.content)
    print("Descarga completada.")
else:
    print("Archivo ZIP ya existe.")

# -----------------------------
# 2. Descomprimir en carpeta
# -----------------------------
extract_folder = 'youtube_data_0333'

if not os.path.exists(extract_folder):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
    print("Archivo descomprimido en:", extract_folder)
else:
    print("Carpeta ya descomprimida.")

# -----------------------------
# 3. Leer archivo .txt
# -----------------------------
# Tomamos el primer archivo .txt dentro de la carpeta
txt_files = [f for f in os.listdir(extract_folder) if f.endswith('.txt')]
file_to_read = os.path.join(extract_folder, txt_files[0])

# Leer sin columnas, separador \t
df = pd.read_csv(file_to_read, sep='\t', header=None)

# -----------------------------
# 4. Asignar nombres de columnas
# -----------------------------
df.columns = [
    'VideoID', 'Uploader', 'age', 'category', 'length', 'views',
    'rate', 'ratings', 'comments', 'related_ids'
]

# -----------------------------
# 5. Filtrar columnas
# -----------------------------
df = df[['VideoID', 'age', 'category', 'views', 'rate']]

# -----------------------------
# 6. Filtrado de categorías
# -----------------------------
categorias_permitidas = ['Music', 'Comedy', 'Sports']
df_filtrado = df[df['category'].isin(categorias_permitidas)]

# -----------------------------
# 7. Exportar a MongoDB
# -----------------------------
try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client['youtube_db']
    collection = db['videos_filtrados']

    # Limpiar colección anterior si existe
    collection.delete_many({})
    
    # Insertar los datos
    collection.insert_many(df_filtrado.to_dict(orient='records'))
    print(" Datos exportados a MongoDB con éxito.")
except Exception as e:
    print(" Error al exportar a MongoDB:", e)