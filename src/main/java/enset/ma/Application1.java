package ya.kr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.DatagramSocket;

public class App1 {
    public static void main(String[] args) {
        SparkSession ss= SparkSession.builder().appName("Exercice").master("local[*]")
                .getOrCreate();
        Dataset<Row> df = ss.read().format("csv").option("header", true).load("src/main/resources/incidents.csv");
        df.createOrReplaceTempView("incidents");

        Dataset<Row> servInc = df.withColumn("service", df.col("service"));
        Dataset<Row> countServInc = servInc.groupBy("service").count();
        countServInc.show();

        System.out.println("deux années (plus grand nombre d'incidents)");
        Dataset<Row> AnsIncid = df.withColumn("year", df.col("date").substr(0, 4));
        Dataset<Row> countIncid = AnsIncid.groupBy("year").count();
        countIncid = countIncid.sort(countIncid.col("count").desc());
        countIncid.show(2);

    }
}