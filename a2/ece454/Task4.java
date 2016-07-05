package ece454;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {
  public static class TokenizerMapper 
       extends Mapper<Object, Text, NullWritable, MapWritable>{
    
    private MapWritable vectorValues = new MapWritable();
    private Text movie = new Text();
    private Text ratings = new Text();
    private static NullWritable nullKey = NullWritable.get();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    String[] tokens = value.toString().split(",", -1);

    long sum = 0L;
    int count = 0;
    for (int i = 1; i < tokens.length; i++) {
        if (tokens[i].length() > 0) {
            sum += Integer.parseInt(tokens[i]);
            count += 1;
        }
    }
    
    String avg = Double.toString(((double) sum) / count);
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < tokens.length; i++) {
        if (tokens[i].length() > 0) {
            sb.append(tokens[i]).append(",");
        } else {
            sb.append(avg).append(",");
        }
    }
    sb.setLength(sb.length() - 1);
    movie.set(tokens[0]);
    ratings.set(sb.toString());
    vectorValues.put(movie, ratings);
    context.write(nullKey, vectorValues);
    }
  }

  public static class PairCombiner 
      extends Reducer<NullWritable,MapWritable,NullWritable,MapWritable> 
  {
    public void reduce(NullWritable key, Iterable<MapWritable> values, 
                       Context context) throws IOException, 
                       InterruptedException {
       
        MapWritable vectorValues = new MapWritable();
        for (MapWritable map: values) {
            vectorValues.putAll(map);
        } 
        context.write(key, vectorValues);
    }
  }

  
  public static class PairReducer 
       extends Reducer<NullWritable,MapWritable,Text,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(NullWritable key, Iterable<MapWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        HashMap<String, List<Double>> all = new HashMap<String, List<Double>>();
        for (MapWritable map: values) {
            for (Writable movie: map.keySet()) {
                ArrayList<Double> vector = new ArrayList<Double>();
                Text movieT = (Text) movie;
                for (String rating: map.get(movieT).toString().split(",", -1)) {
                    vector.add(Double.parseDouble(rating));
                }
                all.put(movieT.toString(), vector);
            }            
        }
        Set<String> keySet = all.keySet();
        String [] keys = keySet.toArray(new String[keySet.size()]);
        for (int i = 0; i < keys.length; i++) {
            String movie1 = keys[i];
            for (int j = i + 1; j < keys.length; j++) {
                String movie2 = keys[j];
                double dot = 0.0;
                double a = 0.0;
                double b = 0.0;

                List<Double> list1 = all.get(movie1);
                List<Double> list2 = all.get(movie2);
                for (int k = 0; k < list1.size(); k++) {
                    double rating1 = list1.get(k);
                    double rating2 = list2.get(k);
                    dot += rating1 * rating2;
                    a += rating1 * rating1;
                    b += rating2 * rating2;
                }
                double similarity = dot / Math.sqrt(a) / Math.sqrt(b);
                Text pair;
                if (movie1.compareTo(movie2) < 0) {
                    pair = new Text(movie1 + "," + movie2);
                } else {
                    pair = new Text(movie2 + "," + movie1);
                }
                Text sim = new Text(String.format("%1.2f", similarity));
                context.write(pair, sim);
            }
        }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: Task 4 <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Task 4");
    job.setJarByClass(Task4.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(PairCombiner.class);
    job.setReducerClass(PairReducer.class);
    //job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
