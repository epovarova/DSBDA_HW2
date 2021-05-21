package bdtc.lab2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static bdtc.lab2.NewsInteractionsCounter.countNewsInteractions;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class SparkTest {

    final String[] testRow1 = "1,10,13,2,2019-09-24 09:26:03".split(",");
    final String[] testRow2 = "2,10,4,3,2020-11-30 18:11:42".split(",");
    final String[] testRow3 = "3,10,4,2,2020-11-30 18:11:42".split(",");
    final String[] testRow4 = "4,69,12,2,2019-06-18 09:10:21".split(",");

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    StructType schema = DataTypes.createStructType(
            new StructField[]{DataTypes.createStructField("id", StringType, false),
                    DataTypes.createStructField("newsId", StringType, false),
                    DataTypes.createStructField("person_id", StringType, false),
                    DataTypes.createStructField("action", StringType, false),
                    DataTypes.createStructField("last_updated", StringType, false)});

    @Test
    public void testOneInteractions() {
        List<String[]> stringAsList = new ArrayList<>();
        stringAsList.add(testRow1);

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<Row> rowRDD = sc.parallelize(stringAsList).map(RowFactory::create);
        JavaRDD<Row> result = countNewsInteractions(ss.createDataFrame(rowRDD, schema).toDF());
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getInt(0) == 10;
        assert rowList.iterator().next().getInt(1) == 2;
        assert rowList.iterator().next().getLong(2) == 1L;
    }

    @Test
    public void testTwoInteractionsSameNewsIdDifferentAction() {
        List<String[]> stringAsList = new ArrayList<>();
        stringAsList.add(testRow1);
        stringAsList.add(testRow2);

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<Row> rowRDD = sc.parallelize(stringAsList).map(RowFactory::create);
        JavaRDD<Row> result = countNewsInteractions(ss.createDataFrame(rowRDD, schema).toDF());
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);

        assert firstRow.getInt(0) == 10;
        assert firstRow.getInt(1) == 2;
        assert firstRow.getLong(2) == 1L;

        assert secondRow.getInt(0) == 10;
        assert secondRow.getInt(1) == 3;
        assert secondRow.getLong(2) == 1L;

    }

    @Test
    public void testTwoInteractionsSameNewsIdAndAction() {
        List<String[]> stringAsList = new ArrayList<>();
        stringAsList.add(testRow1);
        stringAsList.add(testRow3);

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<Row> rowRDD = sc.parallelize(stringAsList).map(RowFactory::create);
        JavaRDD<Row> result = countNewsInteractions(ss.createDataFrame(rowRDD, schema).toDF());
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getInt(0) == 10;
        assert rowList.iterator().next().getInt(1) == 2;
        assert rowList.iterator().next().getLong(2) == 2L;
    }

    @Test
    public void testTwoInteractionsDifferentNewsId() {

        List<String[]> stringAsList = new ArrayList<>();
        stringAsList.add(testRow1);
        stringAsList.add(testRow4);

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<Row> rowRDD = sc.parallelize(stringAsList).map(RowFactory::create);
        JavaRDD<Row> result = countNewsInteractions(ss.createDataFrame(rowRDD, schema).toDF());
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);

        assert firstRow.getInt(0) == 10;
        assert firstRow.getInt(1) == 2;
        assert firstRow.getLong(2) == 1L;

        assert secondRow.getInt(0) == 69;
        assert secondRow.getInt(1) == 2;
        assert secondRow.getLong(2) == 1L;
    }

    @Test
    public void testFourInteractions() {

        List<String[]> stringAsList = new ArrayList<>();
        stringAsList.add(testRow1);
        stringAsList.add(testRow2);
        stringAsList.add(testRow3);
        stringAsList.add(testRow4);

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<Row> rowRDD = sc.parallelize(stringAsList).map(RowFactory::create);
        JavaRDD<Row> result = countNewsInteractions(ss.createDataFrame(rowRDD, schema).toDF());
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);
        Row thirdRow = rowList.get(2);

        assert firstRow.getInt(0) == 10;
        assert firstRow.getInt(1) == 2;
        assert firstRow.getLong(2) == 2L;

        assert secondRow.getInt(0) == 10;
        assert secondRow.getInt(1) == 3;
        assert secondRow.getLong(2) == 1L;

        assert thirdRow.getInt(0) == 69;
        assert thirdRow.getInt(1) == 2;
        assert thirdRow.getLong(2) == 1L;
    }

}
