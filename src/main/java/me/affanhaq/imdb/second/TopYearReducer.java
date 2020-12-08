package me.affanhaq.imdb.second;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TopYearReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

    // map that holds all the movies count by year
    private static final Map<Integer, Integer> MOVIES_MAP = new HashMap<>();

    private final IntWritable movie = new IntWritable();
    private final IntWritable moviesCount = new IntWritable();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // put all the years in the map to sort later
        values.forEach(value -> {
            String[] data = value.toString().split("[\\s]+");
            MOVIES_MAP.put(
                    Integer.parseInt(data[0]), // the movie year
                    Integer.parseInt(data[1]) // the # of movies for that year
            );
        });

        // getting the top year
        Map.Entry<Integer, Integer> top = MOVIES_MAP.entrySet()
                .stream()
                .max(Map.Entry.comparingByValue())
                .orElseThrow(null);

        // printing out the top year
        movie.set(top.getKey());
        moviesCount.set(top.getValue());
        context.write(movie, moviesCount);
    }
}