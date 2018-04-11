package org.shaw.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
    public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: movieB \t movieA=relation
            //pass data to reducer
            String[] line = value.toString().trim().split("\t");
            context.write(new Text(line[0]), new Text(line[1]));
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: user,movie,rating
            //pass data to reducer
            String[] line = value.toString().trim().split(",");
            context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, IntWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
            //collect the data for each movie, then do the multiplication
            //output: key = user:movie, value=rating*relation
            Map<String, Integer> relation_map = new HashMap<>();
            Map<String, Integer> rating_map = new HashMap<>();
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    String[] movie_relation = value.toString().trim().split("=");
                    relation_map.put(movie_relation[0], Integer.parseInt(movie_relation[1]));
                } else {
                    String[] user_rating = value.toString().trim().split(":");
                    rating_map.put(user_rating[0], Integer.parseInt(user_rating[1]));
                }
            }
            for (Map.Entry<String, Integer> relation_entry : relation_map.entrySet()) {
                for (Map.Entry<String, Integer> rating_entry : rating_map.entrySet()) {
                    String movie = relation_entry.getKey();
                    Integer relation = relation_entry.getValue();
                    String user = rating_entry.getKey();
                    Integer rating = rating_entry.getValue();
                    context.write(new Text(user + ":" + movie), new IntWritable(relation * rating));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Multiplication.class);

        ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(CooccurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
