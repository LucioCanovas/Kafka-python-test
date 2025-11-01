from confluent_kafka import Producer
import json
import time
import random

# Configuración del productor
conf = {
    'bootstrap.servers': 'localhost:9092',  # Dirección del broker Kafka
    'client.id': 'python-producer'          # Identificador del cliente
}

# Crear el objeto productor
producer = Producer(conf)

# Callback opcional que informa si el mensaje fue entregado correctamente
def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(f"Mensaje entregado a {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

# Enviar mensajes en bucle
actions = ["login", "logout", "purchase", "view"]

print("Enviando mensajes a Kafka... (Ctrl+C para salir)")

try:
    while True:
        event = {
            "user_id": random.randint(1, 5),
            "action": random.choice(actions),
            "timestamp": time.time()
        }

        # Convertir el evento a JSON y enviarlo al topic
        producer.produce(
            topic="my-topic",
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )

        # Poll procesa los callbacks internos del cliente
        producer.poll(0)

        time.sleep(1)

except KeyboardInterrupt:
    print("\n Detenido por el usuario.")
finally:
    # Esperar a que se envíen todos los mensajes pendientes
    producer.flush()
