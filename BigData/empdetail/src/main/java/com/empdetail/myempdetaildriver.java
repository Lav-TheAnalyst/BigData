
package com.empdetail;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class myempdetaildriver {
    public static void main(String[] args) throws IOException,
            InterruptedException,ClassNotFoundException{
        Job job = Job.getInstance();
        job.setJarByClass(myempdetaildriver.class);
        job.setMapperClass(myempdetailmapper.class);
        job.setReducerClass(myempdetailreduser.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}






