package content;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.LinkedList;
import java.util.Queue;

public class q4_streaming {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("INF583");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));

        Queue<JavaRDD<String>> queue = new LinkedList<>();
        queue.add(jssc.sparkContext().textFile("integers/integers.txt"));
        JavaDStream<String> stream = jssc.queueStream(queue);

        stream.print();

        jssc.start();
        try {jssc.awaitTermination();} catch (InterruptedException e) {e.printStackTrace();}
    }
}
