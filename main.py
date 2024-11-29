from kafka import KafkaProducer, KafkaConsumer
import json, requests, time, threading, logging
from collections import defaultdict, Counter
from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns

inicio = time.time()

POKEMON_URL = "https://pokeapi.co/api/v2/characteristic/"
USER_URL = "https://randomuser.me/api"
contador = 1

productor = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

client = MongoClient("mongodb://localhost:27017/")  
db = client['base_de_datos']
coleccionA = db['coleccionA']
coleccionB = db['coleccionB']

consumer = KafkaConsumer(
    'topico-a', 'topico-b',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) 
)

consumerA = KafkaConsumer(
    'topico-a', 
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) 
)

consumerB = KafkaConsumer(
    'topico-b',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) 
)



def obtener_data_api_pokemon(url:str, params:None):
    print(POKEMON_URL+str(params)+"/")
    response = requests.get(POKEMON_URL+str(params))
    if response.status_code == 200:
        return response.json()  # Devuelve datos en formato JSON
    else:
        print(f"Error: {response.status_code}")
        return None

def obtener_data_api_user(url:str):
    response = requests.get(USER_URL)
    if response.status_code == 200:
        return response.json().get('results')[0]  # Devuelve datos en formato JSON
    else:
        print(f"Error: {response.status_code}")
        return None

"""while True:
    actual = time.time()
    pokemon = obtener_data_api_pokemon(POKEMON_URL,contador)
    #user = requests.get(user_url)
    if pokemon:
        productor.send('topico-a', value=pokemon)  # Envía datos al topic 'topic-a'
        #print(f"Enviado a topico-a:\n {pokemon}")
        print(f"Enviado a topico-a:\n")
    time.sleep(3.5)
    user = obtener_data_api_user(USER_URL)
    if user:
        productor.send('topico-b', value=user)
        #print(f"Enviado a topico-b:\n {user}")
        print(f"Enviado a topico-b:\n")
    time.sleep(5.5)
    pokemon=None
    user=None
    contador +=1
    if (actual-inicio) > 40:
        break
        """

counter=0
for mensaje in consumerA:
    
    #print(f"Insertando en MongoDB: {datos}")
    if counter<=20:
        datos = mensaje.value  # Datos recibidos del topic
        print(f"Insertando en MongoDB  A")
        coleccionA.insert_one(datos)  # Inserta los datos como un documento en la colección
    else:
        break
    counter+=1

counter=0
for mensaje in consumerB:
    if counter<=20:
        datos = mensaje.value  # Datos recibidos del topic
        #print(f"Insertando en MongoDB: {datos}")
        print(f"Insertando en MongoDB  B")
        coleccionB.insert_one(datos)  # Inserta los datos como un documento en la colección
    else:
        break
    counter+=1

datos_pokemon = list(coleccionA.find())
datos_usuario = list(coleccionB.find())

valores_pokemon = [dato['highest_stat']['name'] for dato in datos_pokemon if 'highest_stat' in dato]
#print(valores_pokemon)

valores_usuarios = [dato['results'][0]['nat'] for dato in datos_usuario if 'results' in dato]
#print(valores_usuarios)

# Contar las nacionalidades
nationality_counts = Counter(valores_usuarios)
nationality_labels = list(nationality_counts.keys())
nationality_values = list(nationality_counts.values())

# Contar valores de highest_stat
stat_counts = Counter(valores_pokemon)
stat_labels = list(stat_counts.keys())
stat_values = list(stat_counts.values())

# Crear la figura con subplots
fig, axes = plt.subplots(1, 2, figsize=(14, 6))  # 1 fila, 2 columnas

# Gráfica de nacionalidades
axes[0].bar(nationality_labels, nationality_values, color='skyblue')
axes[0].set_title("Distribución de Nacionalidades", fontsize=16)
axes[0].set_xlabel("Nacionalidad", fontsize=12)
axes[0].set_ylabel("Frecuencia", fontsize=12)
axes[0].grid(axis='y', linestyle='--', alpha=0.7)

# Gráfica de highest_stat
axes[1].bar(stat_labels, stat_values, color='orange')
axes[1].set_title("Distribución de Highest Stat", fontsize=16)
axes[1].set_xlabel("Highest Stat", fontsize=12)
axes[1].set_ylabel("Frecuencia", fontsize=12)
axes[1].grid(axis='y', linestyle='--', alpha=0.7)

# Ajustar el diseño
plt.tight_layout()

# Mostrar ambas gráficas
plt.show()
