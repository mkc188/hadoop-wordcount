package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BigramInitialCount {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable count = new IntWritable();
        private HashMap<String, Integer> h;
        private static String prev = null;

        protected void setup(Context context) {
            h = new HashMap<String, Integer>();
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (java.util.Map.Entry<String, Integer> e : h.entrySet()) {
                word.set(e.getKey());
                count.set(e.getValue());
                context.write(word, count);
            }
        }

        protected void map(LongWritable key, Text value, Context context) {
            String line = value.toString();
            String curr = null;
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                curr = tokenizer.nextToken();
                if (prev != null) {
                    char c1 = prev.charAt(0);
                    char c2 = curr.charAt(0);
                    if ((c1 >= 65 && c1 <= 90 || c1 >= 97 && c1 <= 122) &&
                        (c2 >= 65 && c2 <= 90 || c2 >= 97 && c2 <= 122)) {
                        String k = c1 + " " + c2;
                        Integer v = h.get(k);
                        h.put(k, v == null ? 1 : v + 1);
                    }
                }
                prev = curr;
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        // Run on a pseudo-distributed node 
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");

        Job job = new Job(conf, "bigraminitialcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setJarByClass(BigramInitialCount.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
