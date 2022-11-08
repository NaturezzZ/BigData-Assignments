package hw3;// Map Reduce word count with hadoop

import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.*;


public class Compression {

    public static class IndexMapper
            extends Mapper<Object, Text, Text, PairOfInts> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            HashMap<String, Integer> words = new HashMap<>();
            //remove all non-alphanumeric characters, case-sensitive
            String cleandoc = value.toString().replaceAll("[^a-zA-Z ]", "");
            StringTokenizer itr = new StringTokenizer(cleandoc);
            while (itr.hasMoreTokens()) {
                String w = itr.nextToken();
                if(words.containsKey(w)){
                    words.put(w, words.get(w) + 1);
                } else {
                    words.put(w, 1);
                }
            }
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            Integer fileno = Integer.valueOf(filename.replaceAll("[^0-9]", ""));
            for (String w : words.keySet()) {
                context.write(new Text(w), new PairOfInts(fileno, words.get(w)));
            }
        }
    }

    public class myPartitioner extends Partitioner<Text, PairOfInts> {
        @Override
        public int getPartition(Text key, PairOfInts value, int numReduceTasks) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }
    public static class IndexReducer
            extends Reducer<Text, PairOfInts, Text, Text> {
        Integer indexcnt = 0;
        Integer longestIndex = 0;
        HashSet<String> wordset = new HashSet<>();
        public void reduce(Text key, Iterable<PairOfInts> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<Integer, Integer> files = new HashMap<>();
            for (PairOfInts val : values) {
                if(files.containsKey(val.getLeftElement())){
                    files.put(val.getLeftElement(), files.get(val.getLeftElement()) + val.getRightElement());
                } else {
                    files.put(val.getLeftElement(), val.getRightElement());
                }
            }

            ArrayList<PairOfInts> list = new ArrayList<PairOfInts>();
            for(Integer fileno : files.keySet()) {
                list.add(new PairOfInts(fileno, files.get(fileno)));
            }
            Collections.sort(list);
            StringBuilder sb = new StringBuilder();
            Integer prev = 0;
            int cnt = 0;
            for(PairOfInts p : list){
                Integer diff = p.getLeftElement() - prev;
                prev = p.getLeftElement();
                cnt += 1;
                sb.append(diff.toString() + ":" + p.getRightElement() + ";");
            }
            if(cnt > longestIndex){
                longestIndex = cnt;
                wordset.clear();
                wordset.add(key.toString());
            }
            else if(cnt == longestIndex){
                wordset.add(key.toString());
            }
            context.write(key, new Text(sb.toString()));
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Total Index Count"), new Text(indexcnt.toString()));
            context.write(new Text("Longest Index"), new Text(longestIndex.toString()));
            context.write(new Text("Words with Longest Index"), new Text(wordset.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.reduces", "1");
        URI uri = URI.create("hdfs://s0:9000");
        FileSystem fs = FileSystem.get(uri, conf);
        if (fs.exists(new Path("/Compression"))) {
            fs.delete(new Path("/Compression"), true);
        }
        Job job1 = Job.getInstance(conf, "Index");
        job1.setJarByClass(Compression.class);
        job1.setMapperClass(IndexMapper.class);
        job1.setPartitionerClass(myPartitioner.class);
        job1.setReducerClass(IndexReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(PairOfInts.class);
        FileInputFormat.addInputPath(job1, new Path("/JC"));
        FileOutputFormat.setOutputPath(job1, new Path("/Compression"));
//        FileInputFormat.setMaxInputSplitSize(job1, 1000000000);
//        FileInputFormat.setMinInputSplitSize(job1, 1000000000);
// record time
        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken: " + (endTime - startTime) + "ms");

        System.exit(0);
    }
}