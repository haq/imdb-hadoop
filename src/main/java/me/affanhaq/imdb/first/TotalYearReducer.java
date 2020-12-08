package me.affanhaq.imdb.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class TotalYearReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private final IntWritable count = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // sum the count for each year
        AtomicInteger sum = new AtomicInteger();
        values.forEach(value -> sum.addAndGet(value.get()));

        // update the context
        count.set(sum.get());
        context.write(key, count);
    }
}