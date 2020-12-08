package me.affanhaq.imdb.second;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopYearMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private final static IntWritable ONE = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(ONE, value);
    }
}