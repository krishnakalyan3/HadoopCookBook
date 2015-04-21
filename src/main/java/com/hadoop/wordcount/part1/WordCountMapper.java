package com.hadoop.wordcount.part1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by krishna on 11/04/15.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer token = new StringTokenizer(value.toString());
        while (token.hasMoreTokens()) {

            word.set(token.nextToken());

            context.write(word, new IntWritable(1));
        }
    }
}
