
import pandas as pd
import sqlite3
from pymongo import MongoClient
import smtplib
from email.message import EmailMessage


df = pd.read_csv('winemag-data-130k-v2.csv', index_col=0)


df.rename(columns={
    'country': 'pais',
    'points': 'puntuacion',
    'price': 'precio',
    'taster_name': 'catador',
    'description': 'descripcion'
}, inplace=True)



# 3.1 continente
def asignar_continente(pais):
    europa = ['Italy', 'France', 'Spain', 'Portugal', 'Germany', 'Austria']
    america = ['US', 'Argentina', 'Chile', 'Canada']
    oceania = ['Australia', 'New Zealand']
    africa = ['South Africa']
    asia = ['Israel', 'Turkey', 'India', 'Japan']

    if pais in europa:
        return 'Europa'
    elif pais in america:
        return 'América'
    elif pais in oceania:
        return 'Oceanía'
    elif pais in africa:
        return 'África'
    elif pais in asia:
        return 'Asia'
    else:
        return 'Otros'

df['continente'] = df['pais'].apply(asignar_continente)


df['rango_precio'] = pd.cut(df['precio'], bins=[0, 20, 50, 100, 10000],
                            labels=['Barato', 'Moderado', 'Caro', 'Lujo'])


df['calidad'] = pd.cut(df['puntuacion'], bins=[0, 85, 90, 95, 100],
                       labels=['Baja', 'Media', 'Alta', 'Excelente'])


reporte1 = df.groupby('continente')['puntuacion'].max().reset_index()


reporte2 = df.groupby('pais').agg({
    'precio': 'mean',
    'descripcion': 'count'
}).rename(columns={
    'precio': 'precio_promedio',
    'descripcion': 'cantidad_reviews'
}).sort_values(by='precio_promedio', ascending=False).reset_index()


reporte3 = df['rango_precio'].value_counts().reset_index()
reporte3.columns = ['rango_precio', 'cantidad']


reporte4 = df.groupby(['continente', 'calidad']).size().reset_index(name='cantidad')


reporte1.to_csv('reporte1_mejor_puntuacion_por_continente.csv', index=False)

reporte2.to_excel('reporte2_precio_reviews.xlsx', index=False)

conn = sqlite3.connect('reportes.db')
reporte3.to_sql('reporte_rango_precio', conn, if_exists='replace', index=False)
conn.close()


try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client['vinos_db']
    collection = db['reporte_continente_calidad']
    collection.delete_many({})
    collection.insert_many(reporte4.to_dict('records'))
    print(" Reporte 4 exportado a MongoDB correctamente.")
except Exception as e:
    print(" Error exportando a MongoDB:", e)


msg = EmailMessage()
msg['Subject'] = 'Reporte de vinos - Precio y Reviews por País'
msg['From'] = 'tucorreo@gmail.com'
msg['To'] = 'destinatario@gmail.com'
msg.set_content('Adjunto el reporte solicitado en Excel.')

with open('reporte2_precio_reviews.xlsx', 'rb') as f:
    msg.add_attachment(f.read(), maintype='application',
                       subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                       filename='reporte2_precio_reviews.xlsx')

try:
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login('tucorreo@gmail.com', 'TU_CLAVE_DE_APP')
        smtp.send_message(msg)
    print(" Correo enviado exitosamente.")
except Exception as e:
    print(" Error al enviar correo:", e)
