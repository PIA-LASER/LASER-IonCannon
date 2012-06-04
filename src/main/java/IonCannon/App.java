package IonCannon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class App extends Configured implements Tool
{
    public int run( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Logger logger = LoggerFactory.getLogger(App.class);

        Path inputPath = new Path("sample/input");
        Path outputPath = new Path("sample/output");

        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "localhost:54311");
        conf.set("fs.default.name", "hdfs://localhost:54310/");

        //sampler configuration
        conf.set("strengthToLinkFactor", "1");
        conf.set("numberOfLinksPerCategory", "1500");

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);

        Job job = new Job(conf);

        job.setMapperClass(SamplingMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(SamplingReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJarByClass(App.class);

        boolean success = job.waitForCompletion(true);

        if(success) {
            logger.info("Sampling finished successfully.");
            return 0;
        }

        return -1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new App(), args);
    }

}
