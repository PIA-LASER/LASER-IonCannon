package IonCannon.mapreduce.mapping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class SampleToUrlMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    private static final String SEPARATOR = "[,]";
    private static Pattern pattern = Pattern.compile(SEPARATOR);
    private static int numberOfLinksPerCategory = 1500;

    @Override
    public void setup(Context context) {
        String numOfLinks = context.getConfiguration().get("numberOfLinksPerCategory");

        if (numOfLinks.length() > 0) {
            int tmp = Integer.parseInt(numOfLinks);

            if (tmp != 0)
                numberOfLinksPerCategory = tmp;
        }
    }

    @Override
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
        String[] parsedConfigs = pattern.split(line.toString());

        long categoryId = Long.parseLong(parsedConfigs[1]);
        long linkId = Long.parseLong(parsedConfigs[2]);


        //category * numberOfLinksPerCategory = offset
        linkId = linkId - (categoryId * numberOfLinksPerCategory);

        context.write(new LongWritable(categoryId), new LongWritable(linkId));
    }
}
