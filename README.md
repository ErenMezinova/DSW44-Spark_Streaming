# DSW44-Spark_Streaming

Пришлось сильно повозиться с настройкой окружения, так как под Windows всё сразу не заработало.

**Важные моменты:**

* долгим опытным путем было обнаружено, что spark будет работать только с python 3.11.8 и ниже (с 3.12 не дружит, а в 3.13 нет модуля typing.io, отчего spark сильно ругается)
* добавить переменную окружения PYSPARK_PYTHON=python и прописать в PATH системный и пользовательский пути к ~\Python311\Scripts\ ,  ~\Python311\
* в код consumer'а нужно добавить os.environ['PYSPARK_PYTHON'] = sys.executable и os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable, иначе будет ошибка cannot run program "python3"
* ну и чтобы можно было смотреть результат работы консьюмера в терминале pyCharm, немного изменила изначальный код 

**Версии:**

python 3.11.8
java 23.0.2 2025-01-21
kafka_2.12-3.9.0
spark-3.4.4-bin-hadoop3.3
hadoop winutils 3.3.5
Windows 11 64-bit

## Запуск Zookeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties  

![](https://github.com/ErenMezinova/DSW44-Spark_Streaming/blob/main/1-Zookeeper.png)

## Запуск Kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties         

![](https://github.com/ErenMezinova/DSW44-Spark_Streaming/blob/main/2-Kafka.png)

## Создаем topic
.\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic netology-spark

## Cмотрим, какие топики есть
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

## Отправка сообщений в консоли
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic HWTopic

## Чтение сообщений в консоли
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic HWTopic --from-beginning
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic netology-spark --from-beginning

## Producer.py

[Producer.py](https://github.com/ErenMezinova/DSW44-Spark_Streaming/blob/main/producer.py)

## Consumer.py

[Consumer.py](https://github.com/ErenMezinova/DSW44-Spark_Streaming/blob/main/consumer.py)

## Результат

![](https://github.com/ErenMezinova/DSW44-Spark_Streaming/blob/main/4-cmd_Join.png)

![](https://github.com/ErenMezinova/DSW44-Spark_Streaming/blob/main/4-cmd_GroupBy.png)



