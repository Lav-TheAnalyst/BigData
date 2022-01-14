package com.min.male.salary.com.min;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class minmalesalmapper extends Mapper {
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[]words=line.split(",");
        String gender=words[3];
        int salary=Integer.parseInt(words[4]);
        if(gender.equalsIgnoreCase("male"))
        {
            context.write(new Text(gender),new IntWritable(salary));
        }

    }
}
