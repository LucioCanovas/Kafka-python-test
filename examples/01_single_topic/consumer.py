from confluent_kafka import Consumer, KafkaError
import json

# Configuración del consumidor
conf = {
    'bootstrap.servers': 'localhost:9092',  # Broker Kafka
    'group.id': 'python-consumer-group',    # Grupo de consumidores (permite paralelismo)
    'auto.offset.reset': 'earliest'         # Desde dónde empezar a leer
}

# Crear consumidor
consumer = Consumer(conf)
consumer.subscribe(['my-topic'])

print(" Escuchando mensajes en 'my-topic'... (Ctrl+C para salir)")

try:
    while True:
        msg = consumer.poll(1.0)  # Espera hasta 1 segundo por un mensaje

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f" Error en mensaje: {msg.error()}")
            continue

        # Decodificar el mensaje
        data = json.loads(msg.value().decode('utf-8'))
        print(f" Mensaje recibido: {data}")

except KeyboardInterrupt:
    print("\n Detenido por el usuario.")
finally:
    consumer.close()
