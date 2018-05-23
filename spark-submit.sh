spark-submit --master mesos://localhost:5050 \
             --class com.kumaranBroadridge.main.App \
             --executor-cores 1 \
             --driver-cores 1 \
             --jars  $(echo /home/hadoop/Mainframe_spark_db2/spark-jars-logs/*.jar | tr ' ' ',') \
               /home/hadoop/workspace/TLESampleDemo/target/TLESampleDemo-0.0.1-SNAPSHOT.jar \
               Pavith \
               Gokul\
