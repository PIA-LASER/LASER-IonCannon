package IonCannon;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SamplingReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    @Override
    public void reduce(IntWritable userId, Iterable<IntWritable> userPrefs, Context context) {

    }
}
