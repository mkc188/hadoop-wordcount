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

public class BigramInitialRF {
    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text word = new Text();
        private DoubleWritable count = new DoubleWritable();
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
                        String k1 = c1 + " " + c2;
                        Integer v1 = h.get(k1);
                        h.put(k1, v1 == null ? 1 : v1 + 1);
                        
                        String k2 = c1 + " " + '*';
                        Integer v2 = h.get(k2);
                        h.put(k2, v2 == null ? 1 : v2 + 1);
                    }
                }
                prev = curr;
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private static final DoubleWritable VALUE = new DoubleWritable();
        private double marginal = 0.0f;

        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0f;
            double threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
            double rf;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            if (key.charAt(2) == '*') {
                marginal = sum;
            } else if ((rf = sum / marginal) >= threshold) {
                VALUE.set(sum / marginal);
                context.write(key, VALUE);
            }
        }
    }

    public static class Partition extends Partitioner<Text, DoubleWritable> {
        public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
            return (key.charAt(0) & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static void main(String[] args) throws Exception {
        // Run on a pseudo-distributed node 
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");
        conf.set("threshold", args[2]);

        Job job = new Job(conf, "bigraminitialrf");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setJarByClass(BigramInitialRF.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
