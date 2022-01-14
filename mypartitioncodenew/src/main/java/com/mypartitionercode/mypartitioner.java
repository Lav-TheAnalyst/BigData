package com.mypartitionercode;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class mypartitioner extends Partitioner<Text,Text> {
    @Override
    public int getPartition(Text text, Text text2, int i) {
        String customValue=text2.toString();
        String[] aCustomValue=customValue.split(",");
        int age=Integer.parseInt(aCustomValue[0]);
        if(age<=20)
        {
            return 0;
        }
        else if(age>20 && age<=40)
        {
            return 1;
        }
        else if(age>40 && age<=50)
        {
            return 2;
        }
        else
        {
            return 4;
        }

    }
}
