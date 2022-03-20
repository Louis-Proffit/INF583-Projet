package partB;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;


import java.util.*;

public class Matrixmul {


    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("exA_spark");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> input_idlabels = sc.textFile("graph/idslabels.txt");
        Map<Integer, Double> vector = input_idlabels.mapToPair(s -> new Tuple2<Integer, Double>(Integer.parseInt(s.split(" ")[0]), 1.0 / 64375)).collectAsMap();

        JavaRDD<String> input = sc.textFile("graph/edgelist.txt");

        JavaPairRDD<Integer, List<String>> interm = input.mapToPair(new PairFunction<String, Integer, List<String>>() {
            @Override
            public Tuple2<Integer, List<String>> call(String s) throws Exception {
                List<String> ligne = new ArrayList<String>(Arrays.asList(s.split(" ")));
                Integer key = Integer.parseInt(ligne.get(0));
                ligne.remove(0);
                return new Tuple2<>(key, ligne);
            }
        });

        //JavaPairRDD<Integer, Double> mat = interm.flatMapValues(s-> s).mapValues(Double::parseDouble).mapValues(vector::get);
    }
};
