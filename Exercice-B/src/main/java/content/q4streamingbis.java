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
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;


public class q4streamingbis {


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

    private static int rho(int hash){  // returns the index i in the bitmap
        int result = 0;
        int divider = 1;
        while (hash % divider == 0){
            divider = 2 * divider;
            result += 1;
        }
        return result;
    }

    public static void q4(JavaDStream<Integer> stream) {
        JavaPairDStream<Integer, Integer> res =
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
