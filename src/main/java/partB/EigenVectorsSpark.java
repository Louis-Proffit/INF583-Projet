package partB;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class EigenVectorsSpark {

    public static final int size = 64375;
    public static final double sizeInv = 1.0 / size;

    private static <K,V> void print(JavaPairRDD<K,V> rdd){
        rdd.foreach(
                new VoidFunction<Tuple2<K, V>>() {
                    @Override
                    public void call(Tuple2<K, V> kvTuple2) throws Exception {
                        System.out.println(kvTuple2._1 + " - " + kvTuple2._2);
                    }
                }
        );
    }

    public static JavaPairRDD<Integer, Double> mult(JavaPairRDD<Integer, Integer> edgesReverse, JavaPairRDD<Integer, Double> vector){

        JavaPairRDD<Integer, Tuple2<Integer, Double>> joinedVectorReverse = edgesReverse.join(vector);

        JavaPairRDD<Integer, Double> joinedVector = joinedVectorReverse.mapToPair(
                new PairFunction<Tuple2<Integer, Tuple2<Integer, Double>>, Integer, Double>() {
                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Integer, Double>> tuple) throws Exception {
                        return new Tuple2<>(tuple._2._1, tuple._2._2);
                    }
                }
        );

        JavaPairRDD<Integer, Double> reducedVector = joinedVector.reduceByKey(
                new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double v1, Double v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        JavaRDD<Double> normsVector = reducedVector.map(
                new Function<Tuple2<Integer, Double>, Double>() {
                    @Override
                    public Double call(Tuple2<Integer, Double> v1) throws Exception {
                        return Math.pow(v1._2, 2);
                    }
                }
        );

        Double normSquare = normsVector.reduce(
                new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double v1, Double v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        double norm = Math.sqrt(normSquare);

        return reducedVector.mapToPair(
                new PairFunction<Tuple2<Integer, Double>, Integer, Double>() {
                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Double> tu) throws Exception {
                        return new Tuple2<>(tu._1, tu._2 / norm);
                    }
                }
        );
    }

    public static Integer getMaxNodeIndex(JavaPairRDD<Integer, Double> vector){
        return vector.reduce(
                new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Double> v1, Tuple2<Integer, Double> v2) throws Exception {
                        return v1._2 < v2._2 ? v2 : v1;
                    }
                }
        )._1;
    }

    public static String getMaxNode(JavaPairRDD<Integer, String> nodes, JavaPairRDD<Integer, Double> vector){
        JavaPairRDD<Integer, Tuple2<Double, String>> join = vector.join(nodes);

        return join.values().reduce(
                new Function2<Tuple2<Double, String>, Tuple2<Double, String>, Tuple2<Double, String>>() {
                    @Override
                    public Tuple2<Double, String> call(Tuple2<Double, String> v1, Tuple2<Double, String> v2) throws Exception {
                        return v1._1 < v2._1 ? v2 : v1;
                    }
                }
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
                new PairFunction<Row, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Row row) throws Exception {
                        String[] split = row.getString(0).split(" ", 2);
                        return new Tuple2<>(Integer.parseInt(split[0]), split[1]);
                    }
                }
        );

        JavaPairRDD<Integer, Integer> edges = rawLinks.javaRDD().flatMapToPair(
                new PairFlatMapFunction<Row, Integer, Integer>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Row row) throws Exception {
                        String content = row.getString(0);
                        List<Tuple2<Integer, Integer>> l = new LinkedList<>();
                        String[] split = content.split(" ");
                        Integer index = Integer.parseInt(split[0]);
                        for (int i = 1 ; i < split.length ; i++){
                            l.add(new Tuple2<>(index, Integer.valueOf(split[i])));
                        }
                        return l.iterator();
                    }
                }
        );

        JavaPairRDD<Integer, Integer> edgesReverse = edges.mapToPair(
                new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                        return new Tuple2<>(integerIntegerTuple2._2, integerIntegerTuple2._1);
                    }
                }
        );

        JavaPairRDD<Integer, Double> vector = rawVector.javaRDD().mapToPair(
                new PairFunction<Row, Integer, Double>() {
                    @Override
                    public Tuple2<Integer, Double> call(Row row) throws Exception {
                        String[] split = row.getString(0).split(" ");
                        return new Tuple2<>(Integer.parseInt(split[0]), Double.parseDouble(split[1])); // or alternatively use it as a constant
                    }
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
