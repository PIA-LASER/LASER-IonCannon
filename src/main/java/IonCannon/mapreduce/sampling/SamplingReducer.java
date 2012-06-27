package IonCannon.mapreduce.sampling;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;

public class SamplingReducer extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    public void reduce(IntWritable userId, Iterable<Text> userPrefs, Context context) throws IOException, InterruptedException{
        for(Text pref : userPrefs) {
            String output = new Integer(userId.get()).toString() + "," + pref.toString();
            long currentTime = System.currentTimeMillis() / 1000L;
            long randomOffset = (long)(Math.random() * ((864000)));

            long timestamp = currentTime-randomOffset;

            output += "," + timestamp;

            context.write(new Text(), new Text(output));
        }
    }
}
