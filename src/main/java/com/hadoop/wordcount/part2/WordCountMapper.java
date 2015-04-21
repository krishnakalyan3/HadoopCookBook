package com.hadoop.wordcount.part2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by krishna on 11/04/15.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\\W+");

        for (int i = 0; i < split.length; i++) {

            context.write(new Text(split[i]), one);

        }

    }
}
