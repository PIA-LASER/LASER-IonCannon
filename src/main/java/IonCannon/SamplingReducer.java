package IonCannon;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SamplingReducer extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    public void reduce(IntWritable userId, Iterable<Text> userPrefs, Context context) throws IOException, InterruptedException{
        for(Text pref : userPrefs) {
            String output = new Integer(userId.get()).toString() + "," + pref.toString();
            context.write(new Text(), new Text(output));
        }
    }
}
