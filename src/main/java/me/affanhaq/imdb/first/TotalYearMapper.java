package me.affanhaq.imdb.first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TotalYearMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private final IntWritable year = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // skip the header line
        if (line.toLowerCase().contains("year")) {
            return;
        }

        // line is of the format
        // imdb_title_id, title, original_title, year, date_published, genre, duration, ...
        List<String> data = splitString(value.toString());

        // setting the context
        year.set(Integer.parseInt(data.get(3)));
        context.write(year, ONE);
    }

    // Solution from: Splitting a csv file with quotes as text-delimiter using String.split()
    // https://stackoverflow.com/questions/15738918/splitting-a-csv-file-with-quotes-as-text-delimiter-using-string-split
    public static List<String> splitString(String s) {
        ArrayList<String> words = new ArrayList<>();
        boolean notInsideComma = true;
        int start = 0;
        for (int i = 0; i < s.length() - 1; i++) {
            if (s.charAt(i) == ',' && notInsideComma) {
                words.add(s.substring(start, i));
                start = i + 1;
            } else if (s.charAt(i) == '"')
                notInsideComma = !notInsideComma;
        }
        words.add(s.substring(start));
        return words;
    }
}