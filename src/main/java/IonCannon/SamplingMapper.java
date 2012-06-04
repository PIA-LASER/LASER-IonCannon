package IonCannon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

public class SamplingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private static final String SEPARATOR = "[,\t]";
    private static Pattern pattern = Pattern.compile(SEPARATOR);
    private int strengthToLinkFactor = 10;
    private int numberOfLinksPerCategory = 1500;

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String strength = conf.get("strengthToLinkFactor");

        if (strength.length() > 0) {
            int tmp = Integer.parseInt(strength);

            if (tmp != 0)
                strengthToLinkFactor = tmp;
        }

        String numOfLinks = conf.get("numberOfLinksPerCategory");

        if (numOfLinks.length() > 0) {
            int tmp = Integer.parseInt(numOfLinks);

            if (tmp != 0)
                numberOfLinksPerCategory = tmp;
        }
    }

    @Override
    public void map(LongWritable line, Text input, Context context) throws IOException, InterruptedException {
        String[] parsedConfigs = pattern.split(input.toString());
        Integer[] config = new Integer[parsedConfigs.length];

        for (int i = 0; i < parsedConfigs.length; i++) {
            config[i] = Integer.parseInt(parsedConfigs[i]);
        }

        for (int i = 0; i < config.length; i++) {
            int numberOfLinksForCategory = config[i] * strengthToLinkFactor;
            HashSet<Integer> linksInCategory = new HashSet<Integer>();

            while (true) {
                //got enough links
                if (linksInCategory.size() >= numberOfLinksForCategory) {
                    break;
                }

                Random random = new Random();

                //next random link index in this category
                int nextLinkIndex = random.nextInt(numberOfLinksPerCategory);

                if (linksInCategory.contains(nextLinkIndex)) {
                    continue;
                } else {
                    linksInCategory.add(nextLinkIndex);
                    String categoryIndex = new Integer(i).toString();
                    String linkIndexInCategory = new Integer(nextLinkIndex).toString();
                    String output = categoryIndex + "," + linkIndexInCategory;

                    context.write(new IntWritable((int) line.get()), new Text(output));
                }
            }
        }
    }
}
