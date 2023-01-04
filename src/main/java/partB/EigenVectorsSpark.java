package partB;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;

public class EigenVectorsSpark {

    public static final int size = 64375;

    public static JavaPairRDD<Integer, Double> mult(JavaPairRDD<Integer, Integer> edgesReverse, JavaPairRDD<Integer, Double> vector){

        JavaPairRDD<Integer, Tuple2<Integer, Double>> joinedVectorReverse = edgesReverse.join(vector);

        JavaPairRDD<Integer, Double> joinedVector = joinedVectorReverse.mapToPair(
                (PairFunction<Tuple2<Integer, Tuple2<Integer, Double>>, Integer, Double>) tuple -> new Tuple2<>(tuple._2._1, tuple._2._2)
        );

        JavaPairRDD<Integer, Double> reducedVector = joinedVector.reduceByKey(
                (Function2<Double, Double, Double>) Double::sum
        );

        JavaRDD<Double> normsVector = reducedVector.map(
                (Function<Tuple2<Integer, Double>, Double>) v1 -> Math.pow(v1._2, 2)
        );

        Double normSquare = normsVector.reduce(
                (Function2<Double, Double, Double>) Double::sum
        );

        double norm = Math.sqrt(normSquare);

        return reducedVector.mapToPair(
                (PairFunction<Tuple2<Integer, Double>, Integer, Double>) tu -> new Tuple2<>(tu._1, tu._2 / norm)
        );
    }

    public static String getMaxNode(JavaPairRDD<Integer, String> nodes, JavaPairRDD<Integer, Double> vector){
        JavaPairRDD<Integer, Tuple2<Double, String>> join = vector.join(nodes);

        return join.values().reduce(
                (Function2<Tuple2<Double, String>, Tuple2<Double, String>, Tuple2<Double, String>>) (v1, v2) -> v1._1 < v2._1 ? v2 : v1
        )._2;
    }

    public static void main(String[] args) throws AnalysisException {

        assert(args.length>0);
        int iterations = Integer.parseInt(args[0]);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder().appName("Java Spark SQL for INF583").config("spark.master", "local[*]").getOrCreate();
        Dataset<Row> rawLinks = spark.read().option("header", "false").option("inferSchema", "true").csv("graph/edgelist.txt");
        Dataset<Row> rawVector = spark.read().option("header", "false").option("inferSchema", "true").csv("graph/vector.txt");
        Dataset<Row> rawNodes = spark.read().option("header", "false").option("inferSchema", "true").csv("graph/idslabels.txt");

        JavaPairRDD<Integer, String> nodes = rawNodes.javaRDD().mapToPair(
                (PairFunction<Row, Integer, String>) row -> {
                    String[] split = row.getString(0).split(" ", 2);
                    return new Tuple2<>(Integer.parseInt(split[0]), split[1]);
                }
        );

        JavaPairRDD<Integer, Integer> edges = rawLinks.javaRDD().flatMapToPair(
                (PairFlatMapFunction<Row, Integer, Integer>) row -> {
                    String content = row.getString(0);
                    List<Tuple2<Integer, Integer>> l = new LinkedList<>();
                    String[] split = content.split(" ");
                    Integer index = Integer.parseInt(split[0]);
                    for (int i = 1 ; i < split.length ; i++){
                        l.add(new Tuple2<>(index, Integer.valueOf(split[i])));
                    }
                    return l.iterator();
                }
        );

        JavaPairRDD<Integer, Integer> edgesReverse = edges.mapToPair(
                (PairFunction<Tuple2<Integer, Integer>, Integer, Integer>) integerIntegerTuple2 -> new Tuple2<>(integerIntegerTuple2._2, integerIntegerTuple2._1)
        );

        JavaPairRDD<Integer, Double> vector = rawVector.javaRDD().mapToPair(
                (PairFunction<Row, Integer, Double>) row -> {
                    String[] split = row.getString(0).split(" ");
                    return new Tuple2<>(Integer.parseInt(split[0]), Double.parseDouble(split[1])); // or alternatively use it as a constant
                }
        );

        Util.start();
        double start = System.currentTimeMillis();
        for(int i = 0 ; i < iterations ; i++){
            vector = mult(edgesReverse, vector);
            Util.step();
        }
        String result = getMaxNode(nodes, vector);
        double end = System.currentTimeMillis();
        Util.end(result, start, end);
    }
}
