package content;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.*;
import scala.Function1;
import scala.Function2;
import scala.Int;
import scala.PartialFunction;
import scala.collection.IterableOnce;

import static org.apache.spark.sql.functions.col;

public class Main {

    private static void displayResult(int question, Object result){
        System.out.println(result);
        System.out.println();
    }

    public static void q1(Dataset<Integer> integers){
        Integer max = integers.reduce(
                new ReduceFunction<Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return Math.max(v1,v2);
                    }
                }
        );
        System.out.println("Question 1 - " + max);
    }

    public static void q2(Dataset<Integer> integers){
        Integer sum = integers.reduce(
                new ReduceFunction<Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );
        double average = sum / (double)integers.count();
        System.out.println("Question 2 - " + average);
    }

    public static void q3(SparkSession spark){
        Dataset<Row> result = spark.sql("SELECT DISTINCT(_c0) FROM global_temp.integers");
        System.out.println("Question 3 - ");
        result.show(100);
    }

    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder().appName("Java Spark SQL for Twitter").config("spark.master", "local[*]").getOrCreate();

        Dataset<Integer> integers = spark.read().option("header", "false").option("inferSchema", "true").csv("integers/integers.txt").as(Encoders.INT());
        integers.createGlobalTempView("integers");

        q1(integers);
        q2(integers);
        q3(spark);
    }
}
