package IonCannon;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SamplingMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    @Override
    public void map(LongWritable line, Text input, Context context) {

    }
}
