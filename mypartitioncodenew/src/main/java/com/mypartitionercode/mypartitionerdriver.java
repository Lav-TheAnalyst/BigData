package com.mypartitionercode;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class mypartitionerdriver {
    public static void main(String[] args) throws IOException,
            InterruptedException,ClassNotFoundException{
        Job job = Job.getInstance();
        job.setJarByClass(mypartitionerdriver.class);
        job.setMapperClass(mypartionermapper.class);
        job.setReducerClass(mypartitionerreducer.class);
        job.setPartitionerClass(mypartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);

        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}


