package content;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;

public class MainB {

    public static class IdLabel {

        public int id;
        public String content;

        public IdLabel(int id, String content) {
            this.id = id;
            this.content = content;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }
    }

    public static class EdgeList{

        public Integer id;
        public Integer[] integers;

        public EdgeList(Integer id, Integer[] integers) {
            this.id = id;
            this.integers = integers;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public Integer[] getIntegers() {
            return integers;
        }

        public void setIntegers(Integer[] integers) {
            this.integers = integers;
        }
    }

    public static class Edge{
        public int e1;
        public int e2;

        public Edge(int e1, int e2) {
            this.e1 = e1;
            this.e2 = e2;
        }

        public int getE1() {
            return e1;
        }

        public void setE1(int e1) {
            this.e1 = e1;
        }

        public int getE2() {
            return e2;
        }

        public void setE2(int e2) {
            this.e2 = e2;
        }
    }

    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder().appName("Part B").config("spark.master", "local[*]").getOrCreate();

        Dataset<Row> rawLabels = spark.read().option("header", "false").csv("graph/idslabels.txt");
        Dataset<Row> rawEdges = spark.read().option("header", "false").csv("graph/edgelist.txt");

        Dataset<IdLabel> labels = rawLabels.map(
                new MapFunction<Row, IdLabel>() {
                    @Override
                    public IdLabel call(Row value) throws Exception {
                        String string = value.getString(0);
                        String[] s = string.split(" ", 2);
                        return new IdLabel(Integer.parseInt(s[0]), s[1]);
                    }
                },
                Encoders.bean(IdLabel.class)
        );

        Dataset<EdgeList> edgesAsList = rawEdges.map(
                new MapFunction<Row, EdgeList>() {
                    @Override
                    public EdgeList call(Row value) throws Exception {
                        String[] split = value.getString(0).split(" ",2);
                        String[] edges = split[1].split(" ");
                        Integer[] edgesInt = new Integer[edges.length];
                        Arrays.setAll(edgesInt, new IntFunction<Integer>() {
                            @Override
                            public Integer apply(int value) {
                                return Integer.parseInt(edges[value]);
                            }
                        });
                        return new EdgeList(Integer.parseInt(split[0]), edgesInt);
                    }
                }, Encoders.bean(EdgeList.class)
        );

        Dataset<Edge> edges = edgesAsList.mapPartitions(
                new MapPartitionsFunction<EdgeList, Edge>() {
                    @Override
                    public Iterator<Edge> call(Iterator<EdgeList> input) throws Exception {
                        List<Edge> list = new LinkedList<>();
                        input.forEachRemaining(
                                new Consumer<EdgeList>() {
                                    @Override
                                    public void accept(EdgeList edgeList) {
                                        for (Integer integer : edgeList.integers){
                                            list.add(new Edge(edgeList.id, integer));
                                        }
                                    }
                                }
                        );
                        return list.iterator();
                    }
                }, Encoders.bean(Edge.class)
        );

        labels.javaRDD();

        JavaRDD<Edge> rawEdgeRDD = edges.javaRDD();
        JavaPairRDD<Integer, Integer> edgesRDD = rawEdgeRDD.mapToPair(
                new PairFunction<Edge, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Edge edge) throws Exception {
                        return new Tuple2<>(edge.e1, edge.e2);
                    }
                }
        );


        JavaRDD<IdLabel> labelsRDD = labels.javaRDD();

        int articlesCount = (int)labels.count();
        float initialValue = (float) (1.0 / articlesCount);

        JavaPairRDD<Integer, Float> vectorRDD = labelsRDD.mapToPair(
                new PairFunction<IdLabel, Integer, Float>() {
                    @Override
                    public Tuple2<Integer, Float> call(IdLabel idLabel) throws Exception {
                        return new Tuple2<>(idLabel.id, initialValue);
                    }
                }
        );

        for (int i = 0 ; i < 50 ; i++){
            JavaPairRDD<Integer, Tuple2<Float, Integer>> developped = vectorRDD.join(
                    edgesRDD
            );
            JavaPairRDD<Integer, Tuple2<Float, Integer>> reduced = developped.reduceByKey(
                    new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
                        @Override
                        public Tuple2<Float, Integer> call(Tuple2<Float, Integer> v1, Tuple2<Float, Integer> v2) throws Exception {
                            return new Tuple2<>(v1._1 + v2._1, 0);
                        }
                    }
            );
            vectorRDD = reduced.mapToPair(
                    new PairFunction<Tuple2<Integer, Tuple2<Float, Integer>>, Integer, Float>() {
                        @Override
                        public Tuple2<Integer, Float> call(Tuple2<Integer, Tuple2<Float, Integer>> t) throws Exception {
                            return new Tuple2<>(t._1, t._2._1);
                        }
                    }
            );
        }
    }
}
