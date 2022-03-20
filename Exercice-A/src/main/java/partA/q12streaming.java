package partA;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

public class q12streaming {

    public static JavaDStream<Integer> getStream(JavaStreamingContext jssc){
        Queue<JavaRDD<String>> queue = new LinkedList<>();
        queue.add(jssc.sparkContext().textFile("integers/integers.txt"));
        return jssc.queueStream(queue).map(
                new Function<String, Integer>() {
                    @Override
                    public Integer call(String s) throws Exception {
                        return Integer.valueOf(s);
                    }
                }
        );
    }

    public static void q1(JavaDStream<Integer> str) {
        JavaPairDStream<Integer, Integer> numbers = str.mapToPair(s-> new Tuple2<>(1, s));


        numbers.foreachRDD(new VoidFunction<JavaPairRDD<Integer, Integer>>() {
            @Override
            public void call(JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD) throws Exception {
                Tuple2<Integer, Integer> reduce = integerIntegerJavaPairRDD.reduce(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        return new Tuple2<>(1, Math.max(v1._2, v2._2));
                    }

                });
                System.out.println("Question 1 with max value with spark streaming = "+ reduce._2);
            }
        });

    }

    public static void q2(JavaDStream<Integer> str) {
        JavaPairDStream<Integer, Tuple2<Double, Integer>> numbers_bis = str.mapToPair(new PairFunction<Integer, Integer, Tuple2<Double, Integer>>() {
            @Override
            public Tuple2<Integer, Tuple2<Double, Integer>> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Tuple2<Double, Integer>>(1, new Tuple2<Double, Integer>(Double.parseDouble(integer.toString()), 1));
            }
        });
        JavaPairDStream<Integer, Tuple2<Double, Integer>> sum = numbers_bis.reduceByKey(new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
            @Override
            public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2) throws Exception {
                return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
            }
        });

        sum.foreachRDD(new VoidFunction<JavaPairRDD<Integer, Tuple2<Double, Integer>>>() {
            @Override
            public void call(JavaPairRDD<Integer, Tuple2<Double, Integer>> integerTuple2JavaPairRDD) throws Exception {
                Tuple2<Integer, Tuple2<Double, Integer>> result = integerTuple2JavaPairRDD.reduce(new Function2<Tuple2<Integer, Tuple2<Double, Integer>>, Tuple2<Integer, Tuple2<Double, Integer>>, Tuple2<Integer, Tuple2<Double, Integer>>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<Double, Integer>> call(Tuple2<Integer, Tuple2<Double, Integer>> v1, Tuple2<Integer, Tuple2<Double, Integer>> v2) throws Exception {
                        return new Tuple2<>(1, new Tuple2<>(v1._2._1 / v1._2._2, v1._2._2));
                    }
                });
                System.out.println("Question 2 with average value with spark streaming = "+ result._2._1/result._2._2);
            }
        });


    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("INF583");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaDStream<Integer> stream = getStream(jssc);

        q1(stream);

        q2(stream);

        jssc.start();
        try {jssc.awaitTermination();} catch (InterruptedException e) {e.printStackTrace();}
    }
}
