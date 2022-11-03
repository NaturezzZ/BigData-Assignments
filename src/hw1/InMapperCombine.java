package hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperCombine {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Map<String, Integer> wordmap;
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            wordmap = new HashMap<>();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String w = itr.nextToken();
                wordmap.put(w, wordmap.getOrDefault(w, 0) + 1);
            }
            wordmap.forEach((w, i) -> {
                try {
                    context.write(new Text(w), new IntWritable(i));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.100.3:9000"), conf);
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(InMapperCombine.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}