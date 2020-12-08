package me.affanhaq.imdb;

import me.affanhaq.imdb.first.TotalYearMapper;
import me.affanhaq.imdb.first.TotalYearReducer;
import me.affanhaq.imdb.second.TopYearMapper;
import me.affanhaq.imdb.second.TopYearReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class IMDB {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Missing parameters.");
            System.exit(-1);
        }

        Job firstJob = createJob(
                IMDB.class,
                TotalYearMapper.class,
                TotalYearReducer.class,
                IntWritable.class,
                args[0],
                args[1]
        );

        // we run the first job wait for its completion.
        // if it was successful we run the second job.
        if (firstJob.waitForCompletion(true)) {

            Job secondJob = createJob(
                    IMDB.class,
                    TopYearMapper.class,
                    TopYearReducer.class,
                    Text.class,
                    args[1],
                    args[2]
            );

            System.exit(
                    secondJob.waitForCompletion(true) ? 0 : -1
            );
        }

        // first job was not successful so we exit with negative value
        System.exit(-1);
    }

    private static Job createJob(Class<?> mainClass,
                                 Class<? extends Mapper> mapperClass,
                                 Class<? extends Reducer> reducerClass,
                                 Class<?> mapOutputValueClass,
                                 String inputPath,
                                 String outputPath) throws IOException {
        Job job = new Job();
        job.setJarByClass(mainClass);

        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);

        //
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(mapOutputValueClass);

        //
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set the input and output files
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }
}
