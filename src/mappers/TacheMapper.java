package mappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TacheMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] datas = line.split(",");
        MapWritable datasMap = new MapWritable();

        datasMap.put(new Text("startTime"), new Text(datas[4]));
        datasMap.put(new Text("endTime"), new Text(datas[5]));
        context.write(new Text(datas[1]), datasMap);
    }
    
}
