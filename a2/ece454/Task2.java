package ece454;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringUtils;

public class Task2 {
    public static class TokenizerMapper 
             extends Mapper<Object, Text, NullWritable, IntWritable>{

        private NullWritable nullKey = NullWritable.get();
        private IntWritable finalCount = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",", -1);

            int count = 0;
            for (int i = 1; i < tokens.length; i++) {
                if (tokens[i].length() > 0) {
                    count += 1;
                }
            }

            finalCount.set(count);
            context.write(nullKey, finalCount);
        }
    }

    public static class IntSumReducer
            extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
            private IntWritable finalCount = new IntWritable();

            public void reduce(NullWritable key, Iterable<IntWritable> values,
                    Context context) throws IOException, InterruptedException {
                int count = 0;
                for (IntWritable val : values) {
                    count += val.get();
                }
                finalCount.set(count);
                context.write(key, finalCount);
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Task2 <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Rating Count");
        job.setJarByClass(Task2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
