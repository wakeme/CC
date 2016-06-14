package com.wakeme.homework1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * User: wake
 * Date: 2016/06/13
 * Time: 22:28
 */

public class PairsMapReduce {
    private static final Logger log = Logger.getLogger(PairsMapReduce.class);

    public static void main(String[] args) throws Exception {
        if (args.length() < 2) {
            log.error("Usage: hadoop jar .class_file <input> <output>");
        }
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(PairsMapReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        doMapReduce(job, args[0], PairsOccurrenceMapper.class, args[1], "pairs-co-occur", PairsReducer.class);
    }

    private static void doMapReduce(Job job, String path, Class<? extends Mapper> mapperClass, String outPath, String jobName, Class<? extends Reducer> reducerClass) throws Exception {
        try {
            job.setJobName(jobName);
            FileInputFormat.addInputPath(job, new Path(path));
            FileOutputFormat.setOutputPath(job, new Path(outPath));
            job.setMapperClass(mapperClass);
            job.setReducerClass(reducerClass);
            job.setCombinerClass(reducerClass);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            log.error("Error running MapReduce Job", e);
        }
    }
}


class PairsOccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text wordPair = new Text();
    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // convert string into lower-case
        String line = value.toString().toLowerCase();
        // remove all punctuation
        line = line.replaceAll("[^a-z]", "").trim();
        // split theis line into words
        String[] word = line.toString().split("\\s+");
        // get all word pairs occurr in the same line
        // and save them in format of [word1, word2]
        if (word.length > 1) {
            for (int i = 0; i < word.length; i++) {
                if (word[i] != null && word[i] != "" && word[i].length() > 0) {
                    for (int j = 0; j < word.length; j++) {
                        if (j == i) continue;
                        if (word[j] != null && word[j] != "" && word[j].length() > 0) {
                            wordPair.set("[" + word[i] + ", " + word[j] + "]");
                            context.write(wordPair, ONE);
                        }
                    }
                }
            }
        }
    }
}

class PairsReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable totalCount = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
             count += value.get();
        }
        totalCount.set(count);
        context.write(key,totalCount);
    }
}

