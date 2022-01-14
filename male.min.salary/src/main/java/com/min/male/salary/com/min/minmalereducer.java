package com.min.male.salary.com.min;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class minmalereducer extends Reducer<Text, IntWritable,Text,IntWritable>  {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {


    int min = Integer.MAX_VALUE;
        for (IntWritable value : values) {
            int salary = value.get();
            if (salary < min) ;
            {
                min = salary;
            }

        }
        context.write(key, new IntWritable(min));
    }
}
