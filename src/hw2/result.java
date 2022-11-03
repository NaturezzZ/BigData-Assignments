package hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

import static java.lang.Math.abs;

public class result {
    public static void main(String[] args) {
        Double max = -100.0;
        HashSet<Double> PMI = new HashSet<>();
        HashSet<String> maxPair = new HashSet<>();
        try {
            Configuration conf = new Configuration();
            URI uri = URI.create("hdfs://s0:9000");
            FileSystem fs = FileSystem.get(uri, conf);
            FSDataInputStream fsInput =
                    fs.open(new Path("/PairPMI/part-r-00000"));
            InputStreamReader fsReader = new InputStreamReader(fsInput);
            BufferedReader buffReader = new BufferedReader(fsReader);
            String line;
            while ((line = buffReader.readLine()) != null) {
                String[] words = line.split("\\s+");
//                String[] pair = words[0].split(",");
//                String pair1 = pair[0].substring(1);
//                String pair2 = pair[1].substring(0, pair[1].length() - 1);
                Double val = Double.parseDouble(words[2]);
                PMI.add(val);
                if(abs(val - max) < 0.0000000000000001) {
                    maxPair.add(words[0] + words[1]);
                } else if(val > max) {
                    max = val;
                    maxPair.clear();
                    maxPair.add(words[0] + words[1]);
                }
            }
            buffReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("The maximum PMI is " + max);
        System.out.println("The number of PMI is " + PMI.size());
        System.out.println("Pairs with maximum PMI are:");
        for(String s : maxPair) {
            System.out.print(s+" ");
        }
    }
}
