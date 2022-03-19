package content;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

public class q4_streaming {

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

    private static int hash(int integer){
        return integer;
    }

    private static int rho(int hash){
        int result = 0;
        int divider = 1;
        while (hash % divider == 0){
           divider = 2 * divider;
           result += 1;
        }
        return result;
    }

    public static long getCountFromBitmap(Set<Integer> bitmap, int L){
        int R = L;
        for (int i = L - 1 ; i >= 0 ; i--){
            if (!bitmap.contains(i))
                R = i;
        }
        return getCountFromR(R);
    }

    public static long getCountFromR(int R){
        double psi = 0.77351;
        return (long) (Math.pow(2,R) / psi);
    }

    public static void q4(JavaDStream<Integer> integers){
        int L = 7; // 2^7 > 100
        JavaDStream<Integer> bitmapStream = integers.map(
                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer) throws Exception {
                        return rho(hash(integer));
                    }
                }
        );
        JavaDStream<Set<Integer>> arrays = bitmapStream.map(
                new Function<Integer, Set<Integer>>() {
                    @Override
                    public Set<Integer> call(Integer integer) throws Exception {
                        Set<Integer> result = new HashSet<>();
                        result.add(integer);
                        return result;
                    }
                }
        );

        Set<Integer> bitmap = new HashSet<>();
        arrays.foreachRDD(
                new VoidFunction<JavaRDD<Set<Integer>>>() {
                    @Override
                    public void call(JavaRDD<Set<Integer>> setJavaRDD) throws Exception {
                        if (!setJavaRDD.isEmpty()){
                            Set<Integer> integers = setJavaRDD.reduce(
                                    new Function2<Set<Integer>, Set<Integer>, Set<Integer>>() {
                                        @Override
                                        public Set<Integer> call(Set<Integer> integers, Set<Integer> integers2) throws Exception {
                                            integers.addAll(integers2);
                                            return integers;
                                        }
                                    }
                            );
                            bitmap.addAll(integers);
                        }
                    }
                }
        );

        System.out.println("Question 4 - " + getCountFromBitmap(bitmap, L));
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("INF583");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaDStream<Integer> integers = getStream(jssc);

        q4(integers);

        jssc.start();
        try {jssc.awaitTermination();} catch (InterruptedException e) {e.printStackTrace();}
    }
}
