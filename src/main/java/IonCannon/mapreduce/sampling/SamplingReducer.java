package IonCannon.mapreduce.sampling;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;

public class SamplingReducer extends Reducer<LongWritable, Text, Text, Text> {

    @Override
    public void reduce(LongWritable userId, Iterable<Text> userPrefs, Context context) throws IOException, InterruptedException {
        for (Text pref : userPrefs) {
            String output = new Long(userId.get()).toString() + "," + pref.toString();

            context.write(new Text(), new Text(output));
        }
    }
}
