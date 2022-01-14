package com.mypartitionercode;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class mypartionermapper extends Mapper<IntWritable, Text,Text,Text> {
    @Override
    protected void map(IntWritable key, Text value, Mapper<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        {
            String line=value.toString();
            String[]words=line.split(",");
            String gender=words[3];

            String customvalue =words[2]+"," +words[4];
            context.write(new Text(gender),new Text(customvalue));
        }
    }
}
