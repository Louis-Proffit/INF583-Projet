package partB;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static partB.Util.getEdges;
import static partB.Util.getNodes;

public class EigenVectorsThreads {

    public static Float[] getVector(int size){
        float value = (float) (1.0 / size);
        Float[] result = new Float[size];
        for (int i = 0 ; i < size ; i++){
            result[i] = value;
        }
        return result;
    }

    public static Float[] prod(final HashMap<Integer, Integer[]> matrix, final Float[] vector, int threadsCount, final int size){

        assert(matrix != null);
        int interPerThread = size / threadsCount;

        final Float[] result = new Float[vector.length];
        Thread[] threads = new Thread[threadsCount];
        for (int i = 0 ; i < threadsCount ; i++){
            final Integer threadStart = interPerThread * i;
            final Integer threadEnd = (i == threadsCount - 1 ?size : size / threadsCount * (i + 1));
            Thread thread = new Thread(
                    new Runnable() {
                        @Override
                        public void run() {
                            for (int i = threadStart ; i < threadEnd ; i++){
                                Integer[] edgesOnLine;
                                synchronized (matrix){
                                    edgesOnLine = matrix.get(i);
                                }
                                float sum = 0f;
                                if (edgesOnLine != null){
                                    for (Integer edgeIndex : edgesOnLine){
                                        if (edgeIndex < size)
                                            sum += vector[edgeIndex];
                                    }
                                }
                                result[i] = sum;
                            }
                        }
                    }
            );
            threads[i] = thread;
            thread.start();
        }
        Util.joinThreads(threads);
        return result;
    }

    public static Float[] normalize(final Float[] vector, int threadsCount, int size){
        final Float[] sums = new Float[threadsCount];
        Thread[] threads = new Thread[threadsCount];

        int iterPerThread = size / threadsCount;

        for (int i = 0 ; i < threadsCount ; i++){
            final Integer threadStart = iterPerThread * i;
            final Integer threadEnd = (i == threadsCount - 1 ? size : size / threadsCount * (i + 1));
            final int finalI = i;
            Thread thread = new Thread(
                    new Runnable() {

                        @Override
                        public void run() {
                            float result = 0;
                            for (int i = threadStart ; i < threadEnd ; i++){
                                result += Math.pow(vector[i], 2);
                            }
                            sums[finalI] = result;
                        }
                    }
            );
            threads[i] = thread;
            thread.start();
        }
        Util.joinThreads(threads);
        float sum = 0;
        for (int i = 0 ; i < threadsCount ; i++){
            sum += sums[i];
        }
        final float norm = (float) Math.sqrt(sum);
        for (int i = 0 ; i < threadsCount ; i++){
            final Integer threadStart = size / threadsCount * i;
            final Integer threadEnd = (i == threadsCount - 1 ? size : size / threadsCount * (i + 1));
            Thread thread = new Thread(
                    new Runnable() {

                        @Override
                        public void run() {
                            for (int i = threadStart ; i < threadEnd ; i++){
                                vector[i] = vector[i] / norm;
                            }
                        }
                    }
            );
            threads[i] = thread;
            thread.start();
        }

        Util.joinThreads(threads);
        return vector;
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

    public static void main(String[] args) {

        int iterations = Integer.parseInt(args[0]);
        int numThreads = 2;

        HashMap<Integer, Integer[]> edges;
        HashMap<Integer, String> nodes;
        Float[] vector;
        int size;
        try {
            edges = Util.getEdges();
            nodes = Util.getNodes();
            size = nodes.size();
            vector = getVector(size);
        } catch (FileNotFoundException e) {
            System.err.println("File not found");
            return;
        }

        Util.start();
        double start = System.currentTimeMillis();
        for (int i = 0 ; i < iterations ; i++){
            vector = prod(edges, vector, numThreads, size);
            normalize(vector, numThreads, size);
            Util.step();
        }

        String result = getMostPopularArticle(nodes, vector, size);
        double end = System.currentTimeMillis();

        Util.end(result, start, end);
    }
}
