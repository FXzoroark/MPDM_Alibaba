package mappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;


public class Step1Mapper extends Mapper<LongWritable, Text, Text, MapWritable> {

    private boolean getTerminated;
    private boolean getFailed;
    @Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration() ;
        //utilse pour l'étape 5
		this.getTerminated = conf.getBoolean("getTerminated", true);
		this.getFailed     = conf.getBoolean("getFailed", true);
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] datas = line.split(",");
        String status = datas[3];

        if((!getTerminated && status.equals("Terminated")) || (!getFailed && status.equals("Failed"))) return;

        MapWritable taskMap = new MapWritable();
        MapWritable jobMap = new MapWritable();

        taskMap.put(new Text("startTime"), new Text(datas[4]));
        taskMap.put(new Text("endTime"), new Text(datas[5]));

        jobMap.put(new Text("tacheName"), new Text(datas[1]));
        jobMap.put(new Text("instanceName"), new Text(datas[0]));

        context.write(new Text(datas[1]), taskMap);
        context.write(new Text(datas[2]), jobMap);
    }
    
}
