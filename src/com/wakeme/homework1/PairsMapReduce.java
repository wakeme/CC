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
 * User: Wake
 * Date: 06/13/16
 * Time: 10:03 PM
 */

public class PairsMapReduce {
    private static final Logger log = Logger.getLogger(PairsMapReduce.class);

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(PairsMapReduce.class);
        job.setOutputKeyClass(WordPair.class);
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


class PairsOccurrenceMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
    private WordPair wordPair = new WordPair();
    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().replaceAll("\\pP", " ");
        line = line.toString().replaceAll("\\pN", " ");
        line = line.toString().replaceAll("\\pS", " ");
        line = line.toLowerCase();
        String[] tokens = line.toString().split("\\s+");
        if (tokens.length > 1) {
            for (int i = 0; i < tokens.length; i++) {
                if (tokens[i] != null && tokens[i] != "" && tokens[i].length() > 0) {
                    wordPair.setWord(tokens[i]);
                    for (int j = 0; j < tokens.length; j++) {
                        if (j == i) continue;
                        if (tokens[j] != null && tokens[j] != "" && tokens[j].length() > 0) {
                            wordPair.setNeighbor(tokens[j]);
                            context.write(wordPair, ONE);
                        }
                    }
                }
            }
        }
    }
}

class PairsReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
    private IntWritable totalCount = new IntWritable();

    @Override
    protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
             count += value.get();
        }
        totalCount.set(count);
        context.write(key,totalCount);
    }
}

class WordPair implements Writable, WritableComparable<WordPair> {

    private Text word;
    private Text neighbor;

    public WordPair(Text word, Text neighbor) {
        this.word = word;
        this.neighbor = neighbor;
    }

    public WordPair(String word, String neighbor) {
        this(new Text(word),new Text(neighbor));
    }

    public WordPair() {
        this.word = new Text();
        this.neighbor = new Text();
    }

    public int compareTo(WordPair other) {
        int returnVal = this.word.compareTo(other.getWord());
        if(returnVal != 0){
            return returnVal;
        }
        if(this.neighbor.toString().equals("*")){
            return -1;
        }else if(other.getNeighbor().toString().equals("*")){
            return 1;
        }
        return this.neighbor.compareTo(other.getNeighbor());
    }

    public WordPair read(DataInput in) throws IOException {
        WordPair wordPair = new WordPair();
        wordPair.readFields(in);
        return wordPair;
    }

    public void write(DataOutput out) throws IOException {
        word.write(out);
        neighbor.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        neighbor.readFields(in);
    }

    @Override
    public String toString() {
        return "["+word+", "+neighbor+"]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordPair wordPair = (WordPair) o;

        if (neighbor != null ? !neighbor.equals(wordPair.neighbor) : wordPair.neighbor != null) return false;
        if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = word != null ? word.hashCode() : 0;
        result = 163 * result + (neighbor != null ? neighbor.hashCode() : 0);
        return result;
    }

    public void setWord(String word){
        this.word.set(word);
    }
    public void setNeighbor(String neighbor){
        this.neighbor.set(neighbor);
    }

    public Text getWord() {
        return word;
    }

    public Text getNeighbor() {
        return neighbor;
    }
}
