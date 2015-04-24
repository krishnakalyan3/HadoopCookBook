package com.hadoop.minmax;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by krishna on 24/04/15.
 */
public class MinMaxMapper extends Mapper<LongWritable,IntWritable,Text,IntWritable>{


    @Override
    protected void map(LongWritable key, IntWritable value, Context context) throws IOException, InterruptedException {

        context.write(new Text("Temprature"),value);

    }
}
