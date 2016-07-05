package ece454;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringUtils;

public class Task1 {
    public static class TokenizerMapper 
             extends Mapper<Object, Text, Text, Text>{
        
        private Text title = new Text();
        private Text ratings = new Text();
            
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",", -1);
            
            int max = 0;
            int rating = 0;
            ArrayList<Integer> users = new ArrayList<Integer>();
            title.set(tokens[0]);

            for (int i = 1; i < tokens.length; i++) {
                if (tokens[i].length() > 0) {
                    rating = Integer.parseInt(tokens[i]);
                    if (rating == max) {
                        users.add(i);
                    } else if (rating > max) {
                        users.clear();
                        max = rating;
                        users.add(i);
                    }
                }
            }

            ratings.set(StringUtils.join(users, ','));
            context.write(title, ratings);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Task1 <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Task 1");
        job.setJarByClass(Task1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
