package ece454;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringUtils;

public class Task3 {
    public static class TokenizerMapper 
             extends Mapper<Object, Text, IntWritable, Text>{

        private IntWritable user  = new IntWritable();
        private Text output = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",", -1);

            for (int i = 1; i < tokens.length; i++) {
                if (tokens[i].length() > 0) {
                    user.set(i);
                    output.set(tokens[i] + ",1");
                    context.write(user, output);
                }
            }
        }
    }

    public static class AvgCombiner
            extends Reducer<IntWritable, Text, IntWritable, Text> {

            private Text output = new Text();

            public void reduce(IntWritable key, Iterable<Text> values,
                    Context context) throws IOException, InterruptedException {
                long sum = 0L;
                long length = 0L;
                for(Text value: values) {
                    String[] tokens = value.toString().split(",", -1);
                    sum += Integer.parseInt(tokens[0]);
                    length += Integer.parseInt(tokens[1]);
                }
                output.set(Long.toString(sum) + "," + Long.toString(length));
                context.write(key, output);
            }
    }

    public static class DoubleAvgReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
            private Text avg = new Text();

            public void reduce(IntWritable key, Iterable<Text> values,
                    Context context) throws IOException, InterruptedException {
                Double sum = 0.0;
                long length = 0;
                for(Text value: values) {
                    String[] tokens = value.toString().split(",", -1);
                    sum += Integer.parseInt(tokens[0]);
                    length += Integer.parseInt(tokens[1]);
                }
                avg.set(String.format("%1.1f", sum/length));
                context.write(key, avg);
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Task3 <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Rating Count");
        job.setJarByClass(Task3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(AvgCombiner.class);
        job.setReducerClass(DoubleAvgReducer.class);
        //job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
