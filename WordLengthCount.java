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

public class WordLengthCount {
    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private IntWritable length = new IntWritable();
        private IntWritable count = new IntWritable();
        private HashMap<Integer, Integer> h;

        protected void setup(Context context) {
            h = new HashMap<Integer, Integer>();
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (java.util.Map.Entry<Integer, Integer> e : h.entrySet()) {
                length.set(e.getKey());
                count.set(e.getValue());
                context.write(length, count);
            }
        }

        protected void map(LongWritable key, Text value, Context context) {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                Integer k = tokenizer.nextToken().length();
                Integer v = h.get(k);
                h.put(k, v == null ? 1 : v + 1);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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

        Job job = new Job(conf, "wordlengthcount");

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setJarByClass(WordLengthCount.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
