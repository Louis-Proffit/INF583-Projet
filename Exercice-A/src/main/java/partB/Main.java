package partB;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.*;
import scala.Function1;

import java.util.ArrayList;
import java.util.List;

public class Main {


    /*public static JavaPairRDD<Integer, Float> step(JavaPairRDD<Integer, Float> vector, JavaPairRDD<Integer, Float> eigenMatrix){
    }*/

    public static void main(String[] args) throws AnalysisException {

        assert(args.length>0);
        int iterations = Integer.parseInt(args[0]);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder().appName("Java Spark SQL for Twitter").config("spark.master", "local[*]").getOrCreate();
        Dataset<Row> links = spark.read().option("header", "false").option("inferSchema", "true").csv("graph/edgelist.txt");

        int count = (int) links.count();

        List<Float> r = new ArrayList<>(count);
        float initialValue = 1.0f / count;
        for (int i = 0; i < count; i++){
            r.add(initialValue);
        }

        Dataset<Float> dataset = spark.createDataset(r, Encoders.FLOAT());

        dataset.show(10);
    }
}
