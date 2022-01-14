package com.empdetail;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class mymaxmapper extends Mapper<LongWritable, Text, NullWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), value);
    }
}