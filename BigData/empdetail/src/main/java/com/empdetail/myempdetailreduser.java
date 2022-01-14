package com.empdetail;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myempdetailreduser extends Reducer<NullWritable, Text,NullWritable,Text> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Reducer<NullWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        int max=0;
        String details=null;
        for (Text value:values){
            String line=value.toString();
            int salary=Integer.parseInt(line.split(",")[4]);
            if(salary>max){
                max=salary;
                details=line;

            }

        }
        context.write(NullWritable.get(),new Text(details));
    }
}
