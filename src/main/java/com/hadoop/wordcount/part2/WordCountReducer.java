package com.hadoop.wordcount.part2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by krishna on 22/04/15.
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum =0;
        for(IntWritable val : values){
            sum += val.get();
        }

        context.write(key,new IntWritable(sum));
    }
}
