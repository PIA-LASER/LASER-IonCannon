package IonCannon;

import IonCannon.io.RedisOutputFormat;
import IonCannon.mapreduce.mapping.SampleToUrlMapper;
import IonCannon.mapreduce.mapping.SampleToUrlReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import redis.clients.jedis.Jedis;


import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class SampleToUrlReducerTest {

    @Test
    public void testSetup() throws IOException{
        Configuration conf = new Configuration();
        Reducer.Context context = mock(Reducer.Context.class);

        stub(context.getConfiguration()).toReturn(conf);


        conf.set("fs.default.name", "hdfs://localhost:54310/");


        SampleToUrlReducer reducer = new SampleToUrlReducer();
        reducer.setup(context);
    }

    @Test
    public void testMapper() throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("numberOfLinksPerCategory", "50");

        Mapper.Context context = mock(Mapper.Context.class);

        stub(context.getConfiguration()).toReturn(conf);

        LongWritable key = new LongWritable(1);
        Text text = new Text("12,2,105");

        SampleToUrlMapper mapper = new SampleToUrlMapper();
        mapper.setup(context);
        mapper.map(key,text,context);
    }

    @Test
    public void testReducer() throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("numberOfLinksPerCategory", "50");
        conf.set("fs.default.name", "hdfs://localhost:54310/");

        Reducer.Context context = mock(Reducer.Context.class);

        stub(context.getConfiguration()).toReturn(conf);

        LongWritable key = new LongWritable(2);
        LongWritable text = new LongWritable(5);
        ArrayList<LongWritable> al = new ArrayList<LongWritable>();
        al.add(text);
        SampleToUrlReducer reducer = new SampleToUrlReducer();
        reducer.setup(context);
        reducer.reduce(key, al, context);
    }

    @Test
    public void RedisTest() {
        Jedis con = new Jedis("127.0.0.1");

        con.rpush("blabla","http://www.welt.de/politik/ausland/article107135604/Tuerkei-beraet-ueber-Reaktion-auf-Flieger-Abschuss.html");
        con.rpush("blabla","Türkei berät über Reaktion auf Flieger-Abschuss");
    }
}
