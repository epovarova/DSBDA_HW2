package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Считает количество  взаимодействия пользователей с новостной лентой для каждой новости за все время.
 */
@Slf4j
public class SparkSQLApplication {

    /**
     * @param args - args[0]:  - выходная папка
     */
    public static void main(String[] args) {
        log.info("Appliction started!");
        log.debug("Application started");
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/testDB";
        String user = "liza";
        String pass = "password";

        SparkSession sc = SparkSession
                .builder()
                .master("local")
                .appName("SparkSQLApplication")
                .getOrCreate();

        Dataset<Row> df =
                sc.read().format("jdbc")
                        .option("driver", driver)
                        .option("url", url)
                        .option("dbtable", "NewsInteractions")
                        .option("user", user)
                        .option("password", pass)
                        .load();
        log.info("===============COUNTING...================");
        JavaRDD<Row> result = NewsInteractionsCounter.countNewsInteractions(df);
        log.info("============SAVING FILE TO " + args[0] + " directory============");
        result.saveAsTextFile(args[0]);
    }
}
