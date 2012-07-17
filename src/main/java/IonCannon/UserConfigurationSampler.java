package IonCannon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

public class UserConfigurationSampler {
    public static void sampleGaussConfigurations(Configuration config, FileSystem fs, Path targetPath) throws IOException {
        FSDataOutputStream output = fs.create(targetPath);

        int nUsers = Integer.parseInt(config.get("sampler_nUsers"));
        int nTopics = Integer.parseInt(config.get("sampler_nTopics"));
        int maxSpots = Integer.parseInt(config.get("sampler_maxSpots"));
        float defSigma = Float.parseFloat(config.get("sampler_defSigma"));
        float minYValue = Float.parseFloat(config.get("sampler_minYVal"));
        float bufferSize = Float.parseFloat(config.get("sample.buffer.mb"));

        float sizePerUser = nTopics * (32.0f / (8*1024*1024));
        ArrayList users = new ArrayList();
        Random gen = new Random();

        //for each user
        for (int i = 0; i < nUsers; i++) {
            //init topic array with zeros
            float[] topics = new float[nTopics];
            for (int j = 0; j < nTopics; j++)
                topics[j] = 0;
            //how many spots (random)?
            int nSpots = gen.nextInt(maxSpots + 1);
            //we would have at least one spot
            if (nSpots < 1) nSpots = 1;
            for (int k = 0; k < nSpots; k++) {
                //create random mu
                int mu = gen.nextInt(nTopics);
                //modify sigma randomly
                float sigma = defSigma * gen.nextFloat();
                //sigma should be at least 0.1 to prevent extreme high peaks
                sigma = sigma > 0.1f ? sigma : 0.1f;
                //for each topic
                for (int l = 0; l < nTopics; l++) {
                    //calculate y
                    double exp = -0.5 * Math.pow(((l - mu) / sigma), 2);
                    double right = Math.pow(Math.E, exp);
                    double left = 1 / (sigma * Math.sqrt(2 * Math.PI));
                    double y = left * right;
                    //check if value is bigger than old one at this topic
                    if (y > minYValue) {
                        if (y > topics[l])
                            topics[l] = (float) y;
                    }
                }
            }

            users.add(topics);

            float totalSize = users.size() * sizePerUser;

            if (totalSize >= bufferSize) {
                writeSampleBufferToHDFS(fs, output, targetPath, users);
                users = new ArrayList();
            }
        }

        writeSampleBufferToHDFS(fs, output, targetPath,users);
        output.close();
    }

    public static void writeSampleBufferToHDFS(FileSystem fs, FSDataOutputStream output, Path targetPath, ArrayList users) throws IOException{
        Iterator userIter = users.iterator();
        long userID = 0;

        while (userIter.hasNext()) {
            float[] topics = (float[]) userIter.next();

            byte[] userIDString = (new String(Long.toString(userID)) + ",").getBytes();
            output.write(userIDString, 0, userIDString.length);

            for (int j = 0; j < topics.length; j++) {
                if (topics[j] == 0) {
                    byte[] zeroString = new String("0").getBytes();
                    output.write(zeroString, 0, zeroString.length);
                } else {
                    byte[] actualOutput = new Float(topics[j]).toString().getBytes();
                    output.write(actualOutput, 0, actualOutput.length);
                }

                if (j < topics.length - 1) {
                    byte[] sep = ",".getBytes();
                    output.write(sep, 0, sep.length);
                }
            }

            byte[] newline = "\n".getBytes();
            output.write(newline, 0, newline.length);
            output.flush();
            userID++;
        }
    }
}
