package partB;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class Util {

    private static final String edgesPath = "graph/edgelist.txt";
    private static final String nodesPath = "graph/idslabels.txt";

    public static void start(){
        System.out.println("Starting");
    }

    public static void step(){
        System.out.print(".");
    }

    public static void end(String result, double t1, double t2){
        System.out.println();
        System.out.println("Finished");
        //System.out.println("----------------------------");
        System.out.println("Result = \"" + result + "\"");
        //System.out.println("----------------------------");
        System.out.println("Time used for computation : " + (t2 - t1) + "ms");
    }

    public static HashMap<Integer, Integer[]> getEdges() throws FileNotFoundException {

        HashMap<Integer, Integer[]> result = new HashMap<>();

        Scanner scanner = new Scanner(new File(edgesPath));
        while(scanner.hasNext()){
            String line = scanner.nextLine();
            String[] split = line.split(" ");
            Integer[] otherNodes = new Integer[split.length - 1];
            for (int i = 1 ; i < split.length ; i++){
                otherNodes[i - 1] = Integer.parseInt(split[i]);
            }
            result.put(Integer.parseInt(split[0]), otherNodes);
        }
        return result;
    }

    public static HashMap<Integer, String> getNodes() throws FileNotFoundException {

        HashMap<Integer, String> result = new HashMap<>();

        Scanner scanner = new Scanner(new File(nodesPath));
        while(scanner.hasNext()){
            String line = scanner.nextLine();
            String[] split = line.split(" ", 2);
            result.put(Integer.parseInt(split[0]), split[1]);
        }
        return result;
    }

    public static void joinThreads(Thread [] threads){
        try {
            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
