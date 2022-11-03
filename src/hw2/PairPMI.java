package hw2;// Map Reduce word count with hadoop

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import edu.umd.cloud9.io.pair.PairOfStrings;


public class PairPMI {

    // Phase 1: calculate # of files that the word appears in

    public static class Phase1Mapper
            extends Mapper<Object, Text, Text, IntWritable> {
        HashSet<String> wordset = new HashSet<String>();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text word = new Text();
            //remove all non-alphanumeric characters, case-sensitive
            String cleandoc = value.toString().replaceAll("[^a-zA-Z ]", "");
            StringTokenizer itr = new StringTokenizer(cleandoc);
            while (itr.hasMoreTokens()) {
                String w = itr.nextToken();
                wordset.add(w);
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (String w : wordset) {
                context.write(new Text(w), new IntWritable(1));
            }
        }
    }

    public static class Phase1Combiner
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Phase1Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Phase2Mapper
            extends Mapper<Object, Text, PairOfStrings, IntWritable> {
        private TreeSet<String> wordset = new TreeSet<>();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            PairOfStrings pair = new PairOfStrings();
            //remove all non-alphanumeric characters, case-sensitive
            String cleandoc = value.toString().replaceAll("[^a-zA-Z ]", "");
            StringTokenizer itr = new StringTokenizer(cleandoc);
            while (itr.hasMoreTokens()) {
                String w = itr.nextToken();
                wordset.add(w);
            }

        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            ArrayList<String> wordlist = new ArrayList<String>(wordset);
            for(int i = 0; i < wordlist.size(); i++) {
                for(int j = i+1; j < wordlist.size(); j++) {
                    context.write(new PairOfStrings(wordlist.get(i), wordlist.get(j)), new IntWritable(1));
                }
            }
        }
    }

    public static class Phase2Combiner
            extends Reducer<PairOfStrings,IntWritable,PairOfStrings,IntWritable> {
        public void reduce(PairOfStrings key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Phase2Reducer
            extends Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {
        private HashMap<String, Integer> count = new HashMap<>();
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            try {
                FSDataInputStream fsInput =
                        fs.open(new Path("/SingleWordCount/part-r-00000"));
                InputStreamReader fsReader = new InputStreamReader(fsInput);
                BufferedReader buffReader = new BufferedReader(fsReader);

                String line;
                while ((line = buffReader.readLine()) != null) {
                    String[] counting = line.split("\\s+");
                    count.put(counting[0], Integer.parseInt(counting[1]));
                }
                buffReader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        public void reduce(PairOfStrings key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            sum = sum / 146.0;
            double xnum = count.get(key.getLeftElement())/146.0;
            double ynum = count.get(key.getRightElement())/146.0;
            double pmi = Math.log10(sum / (xnum * ynum));
            context.write(key, new DoubleWritable(pmi));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.reduces", "1");
        URI uri = URI.create("hdfs://s0:9000");
        FileSystem fs = FileSystem.get(uri, conf);
        Path singleword_dir = new Path("/SingleWordCount");
        Path pair_dir = new Path("/PairPMI");
        boolean useCombiner = true;
        for(String s : args) {
            if (s.equals("-nocombiner")) {
                useCombiner = false;
                break;
            }
        }
        if (fs.exists(singleword_dir)) {
            fs.delete(singleword_dir, true);
        }
        if (fs.exists(pair_dir)) {
            fs.delete(pair_dir, true);
        }
        Job job1 = Job.getInstance(conf, "PMI Pair Phase 1");
        job1.setJarByClass(PairPMI.class);
        job1.setMapperClass(Phase1Mapper.class);
        if(useCombiner) {
            job1.setCombinerClass(Phase1Combiner.class);
        }
        job1.setReducerClass(Phase1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("/JC"));
        FileOutputFormat.setOutputPath(job1, new Path("/SingleWordCount"));
        FileInputFormat.setMaxInputSplitSize(job1, 1000000000);
        FileInputFormat.setMinInputSplitSize(job1, 1000000000);

        Job job2 = Job.getInstance(conf, "PMI Pair Phase 2");
        job2.setJarByClass(PairPMI.class);
        job2.setMapperClass(Phase2Mapper.class);
        if(useCombiner) {
            job2.setCombinerClass(Phase2Combiner.class);
        }
        job2.setReducerClass(Phase2Reducer.class);
        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(PairOfStrings.class);
        job2.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("/JC"));
        FileOutputFormat.setOutputPath(job2, new Path("/PairPMI"));
        // set file input format as max split size
        FileInputFormat.setMaxInputSplitSize(job2, 1000000000);
        FileInputFormat.setMinInputSplitSize(job2, 1000000000);

        // record time of these 2 jobs
        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        job2.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Job took " + (endTime - startTime) / 1000.0 + " seconds");

        System.exit(0);
    }
}