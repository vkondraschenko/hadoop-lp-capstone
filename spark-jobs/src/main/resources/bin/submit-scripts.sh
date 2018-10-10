spark-submit --master yarn --num-executors 1 --driver-memory 512m --executor-memory 512m --executor-cores 1 --deploy-mode client --jars hdfs://sandbox-hdp.hortonworks.com:8020/spark-app/mysql-connector-java-8.0.12.jar --class com.griddynamics.training.spark.jobs.FrequentlyPurchasedCategories hdfs://sandbox-hdp.hortonworks.com:8020/spark-app/vk-spark-jobs-1.0-SNAPSHOT.jar

spark-submit --master yarn --num-executors 1 --driver-memory 512m --executor-memory 512m --executor-cores 1 --deploy-mode client --jars hdfs://sandbox-hdp.hortonworks.com:8020/spark-app/mysql-connector-java-8.0.12.jar --class com.griddynamics.training.spark.jobs.FrequentlyPurchasedProductsByCategories hdfs://sandbox-hdp.hortonworks.com:8020/spark-app/vk-spark-jobs-1.0-SNAPSHOT.jar

spark-submit --master yarn --num-executors 1 --driver-memory 512m --executor-memory 512m --executor-cores 1 --deploy-mode client --jars hdfs://sandbox-hdp.hortonworks.com:8020/spark-app/mysql-connector-java-8.0.12.jar --class com.griddynamics.training.spark.jobs.TopCountriesWithHighestMoneySpent hdfs://sandbox-hdp.hortonworks.com:8020/spark-app/vk-spark-jobs-1.0-SNAPSHOT.jar

