package partA;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import scala.Tuple2;

public class Spark1234 {

    public static void q1(JavaRDD<Integer> integers){
        Integer max = integers.reduce(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 < v2 ? v2 : v1;
                    }
                }
        );
        System.out.println("Question 1 - " + max);
    }

    public static void q2(JavaRDD<Integer> integers){
        Integer sum = integers.reduce(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );
        double average = sum / (double)integers.count();
        System.out.println("Question 2 - " + average);
    }

    public static void q3(JavaRDD<Integer> integers){
        JavaPairRDD<Integer, Void> pairedIntegers = integers.mapToPair(
                new PairFunction<Integer, Integer, Void>() {
                    @Override
                    public Tuple2<Integer, Void> call(Integer integer) throws Exception {
                        return new Tuple2<>(integer, null);
                    }
                }
        );
        JavaRDD<Integer> result = pairedIntegers.reduceByKey(
                new Function2<Void, Void, Void>() {
                    @Override
                    public Void call(Void v1, Void v2) throws Exception {
                        return null;
                    }
                }
        ).map(
                new Function<Tuple2<Integer, Void>, Integer>() {
                    @Override
                    public Integer call(Tuple2<Integer, Void> v1) throws Exception {
                        return v1._1;
                    }
                }
        );
        System.out.println("Question 3 - ");
        result.foreach(
                new VoidFunction<Integer>() {
                    @Override
                    public void call(Integer integer) throws Exception {
                        System.out.print(integer + " - ");
                    }
                }
        );
        System.out.println();
    }

    public static void q4(JavaRDD<Integer> integers){

        JavaPairRDD<Integer, Void> pairedIntegers = integers.mapToPair(
                new PairFunction<Integer, Integer, Void>() {
                    @Override
                    public Tuple2<Integer, Void> call(Integer integer) throws Exception {
                        return new Tuple2<>(integer, null);
                    }
                }
        );

        JavaPairRDD<Integer, Void> reducedIntegers = pairedIntegers.reduceByKey(
                new Function2<Void, Void, Void>() {
                    @Override
                    public Void call(Void v1, Void v2) throws Exception {
                        return null;
                    }
                }
        );

        long result = reducedIntegers.count();

        System.out.println("Question 4 - " + result);
    }

    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder().appName("Java Spark SQL for INF583").config("spark.master", "local[*]").getOrCreate();

        Dataset<Integer> rawIntegers = spark.read().option("header", "false").option("inferSchema", "true").csv("integers/integers.txt").as(Encoders.INT());
        JavaRDD<Integer> integers = rawIntegers.javaRDD();

        q1(integers);
        q2(integers);
        q3(integers);
        q4(integers);
    }
}
