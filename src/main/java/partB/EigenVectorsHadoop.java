package partB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.*;

public class EigenVectorsHadoop {

    private static final int size = 64675;

    private static class IntFloatWritable implements WritableComparable<IntFloatWritable> {

        public int i;
        public float f;

        public IntFloatWritable(){}

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(i);
            dataOutput.writeFloat(f);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            i = dataInput.readInt();
            f = dataInput.readFloat();
        }

        @Override
        public String toString() {
            return i + " " + f;
        }

        @Override
        public int compareTo(@NotNull EigenVectorsHadoop.IntFloatWritable o) {
            return Integer.compare(i, o.i);
        }
    }

    /*public static class ListFloatWritable implements Writable {

        public List<Integer> l;
        public float value;


        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(l.size());
            for (Integer iw : l) {
                dataOutput.writeInt(iw);
            }
            dataOutput.writeFloat(value);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            int size = dataInput.readInt();
            l = new ArrayList<>(size);
            for (int j = 0 ; j < size ; j++){
                l.add(dataInput.readInt());
            }
            value = dataInput.readFloat();
        }
    }*/


    public static class MapperMatrix
            extends Mapper<Object, Text, IntWritable, IntFloatWritable> {

        private final static IntWritable in = new IntWritable();
        private final static IntFloatWritable ifl = new IntFloatWritable();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] split = value.toString().split(" ");
            in.set(Integer.parseInt(split[0]));
            for(int i = 1 ; i < split.length ; i++){
                ifl.i = Integer.parseInt(split[i]);
                // ifl.setF((float) (1.0 / size));
                context.write(in, ifl);
            }
        }
    }


    public static class MapperMultMatrix extends Mapper<IntWritable, IntFloatWritable, IntWritable, IntFloatWritable>{


        @Override
        protected void map(IntWritable key, IntFloatWritable value, Mapper<IntWritable, IntFloatWritable, IntWritable, IntFloatWritable>.Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    /**
     * Input :
     * - key : i
     * - value : (j, x_j)
     *
     * Output : for all values of j in (j_1, ..., j_n)
     * - key : j
     * - output : (i, x_i)
     */
    public static class ReducerMultMatrix extends Reducer<IntWritable, IntFloatWritable, IntWritable, IntFloatWritable>{

        private final IntFloatWritable ifw = new IntFloatWritable();
        private final IntWritable i = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntFloatWritable> values, Reducer<IntWritable, IntFloatWritable, IntWritable, IntFloatWritable>.Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (IntFloatWritable x : values){
                sum += x.f;
            }

            ifw.i = key.get();
            ifw.f = sum;
            for (IntFloatWritable x : values){
                i.set(x.i);
                context.write(i, ifw);
            }

        }
    }


    public static String getMostPopularArticle(Map<Integer, String> nodes, Float[] vector, int size){
        float max = 0;
        int maxIndex = -1;
        float f;
        for (int i = 0 ; i < size ; i++){
            f = vector[i];
            if(f > max){
                max = f;
                maxIndex = i;
            }
        }
        return nodes.get(maxIndex);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        int iterations = Integer.parseInt(args[0]);

        HashMap<Integer, Integer[]> edges;
        HashMap<Integer, String> nodes;
        int size;
        try {
            edges = Util.getEdges();
            nodes = Util.getNodes();
            size = nodes.size();
        } catch (FileNotFoundException e) {
            System.err.println("File not found");
            return;
        }

        double start = System.currentTimeMillis();


        /*

        TODO

         */

        Configuration conf = new org.apache.hadoop.conf.Configuration();
        String input = "graph/edgelist.txt";
        String output1 = "graph/tmp";
        Job job1 = Job.getInstance(conf, "file to matrix");
        job1.setJarByClass(EigenVectorsHadoop.class);
        job1.setMapperClass(MapperMatrix.class);
        //job1.setReducerClass(ReducerMatrix.class);
        job1.setNumReduceTasks(0);
        //job1.setMapOutputKeyClass(IntWritable.class);
        //job1.setMapOutputValueClass(IntFloatWritable.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntFloatWritable.class);
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output1));
        job1.waitForCompletion(true);

        //String result = getMostPopularArticle(nodes, vector, size);

        double end = System.currentTimeMillis();

        System.out.println("Time spend \t : " + (end - start) + "ms");
        //System.out.println("Result \t \t : " + result);

    }
}
