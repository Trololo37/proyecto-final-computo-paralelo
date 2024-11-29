# proyecto-final-computo-paralelo
## correr:
### docker pull confluentinc/cp-zookeeper
### docker pull apache/kafka
### docker-compose up -d
Se crean las imagenes a medida
### docker exec -it kafka bash
Corremos la imagen de kafka proyecto final
### kafka-topics --create --topic topico-a --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
Con esto se crean los tópicos
### kafka-topics --create --topic topico-b --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
#### kafka-console-consumer --topic topico-a --from-beginning --bootstrap-server localhost:9092
Esto es para estar escuchando y monitorando
#### kafka-console-consumer --topic topico-b --from-beginning --bootstrap-server localhost:9092