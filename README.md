# 💎 This Project is to Demonstrate the Knowledge in  `Data Engineering Domain`.
>▮  This is a live weather Data Monitoring End_to_End Pipeline Project. 🦉


# 🌐 Socials :
[![Behance](https://img.shields.io/badge/Behance-1769ff?logo=behance&logoColor=white)](https://behance.net/Vinay_kumar) [![Instagram](https://img.shields.io/badge/Instagram-%23E4405F.svg?logo=Instagram&logoColor=white)](https://instagram.com/knightkings77) [![LinkedIn](https://img.shields.io/badge/LinkedIn-%230077B5.svg?logo=linkedin&logoColor=white)](https://linkedin.com/in/https://www.linkedin.com/in/vinaykumar77/) 

# 💻 Technoliges I have use in this project :
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![Windows Terminal](https://img.shields.io/badge/Windows%20Terminal-%234D4D4D.svg?style=for-the-badge&logo=windows-terminal&logoColor=white) ![GithubPages](https://img.shields.io/badge/github%20pages-121013?style=for-the-badge&logo=github&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka) ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black) ![Apache Maven](https://img.shields.io/badge/Apache%20Maven-C71A36?style=for-the-badge&logo=Apache%20Maven&logoColor=white) ![AmazonDynamoDB](https://img.shields.io/badge/Amazon%20DynamoDB-4053D6?style=for-the-badge&logo=Amazon%20DynamoDB&logoColor=white)




# 🗿 Flow Chart :
![Data_pipeline](Brainstorming and ideation.png)

# 🧩 Project Flow :

>Install Kafka and Spark !

>Create the account in the OpenWeatherMap Api.

>Watchout for the Dependencies Needed.

>Run your  `ZooKeeper Server` in your Local System.

                        $ bin/zookeeper-server-start.sh config/zookeeper.properties

>Start your `kafka Server` in your Local System.

                        $ bin/kafka-server-start.sh config/server.properties

>If you haven't created `topic` the create one

                        $ bin/kafka-topics.sh --create --topic <Topic_name> --bootstrap-server localhost:9092

>Open up the `producer` in other terminal or run the producer code  `Api_To_Kafka.py`  or  `Api_To_Kafka_With_Pyspark.py`

                        $ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic <Topic_name>

>Now run the `Consumer Ap`p which will consume the data from that Topic or run the consumer code  `Kafka_To_Parquet.py` and `Parquet_To_MongoDB.py`

                        $ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <Topic_name> --from-beginning
>In this project, I have used MongoDB Atlas to store and view real-time data. 
>To integrate MongoDB with the locally running Spark job, you need to download the [`Spark-MongoDB Connector.`](https://www.mongodb.com/docs/spark-connector/current/).
>It is a JAR file that you can find in the provided link or on the [`Maven Dependencies`](https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector) webpage.


# 🎰 Language Type :
![](https://github-readme-stats.vercel.app/api/top-langs/?username=Vinay7k7&theme=dark&hide_border=false&include_all_commits=false&count_private=false&layout=compact)
========================================================================================================================================================================================================
