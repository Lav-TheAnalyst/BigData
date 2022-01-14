package com.mypartitionercode;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class mypartitionerreducer extends Reducer<Text,Text,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int max=0;
        for(Text value:values){
            String customvalue=value.toString();
            String[] aCustomValue=customvalue.split(",");
            int salary=Integer.parseInt(aCustomValue[1]);
            if(max>salary)
            {
                max=salary;
            }
        }
        context.write(key,new IntWritable(max));
    }
}
