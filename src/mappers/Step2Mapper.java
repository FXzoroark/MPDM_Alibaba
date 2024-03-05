package mappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step2Mapper extends Mapper<LongWritable, Text, Text, MapWritable> {

	private int rowKey ;	

	@Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration() ;
        //utilse pour l'étape 3
		this.rowKey = Integer.parseInt(conf.get("rowKey"));
		
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] datas = line.split(",");
        if(datas.length == 11){
            MapWritable taskMap = new MapWritable();

            taskMap.put(new Text("startTime"), new Text(datas[4]));
            taskMap.put(new Text("endTime"), new Text(datas[5]));
            taskMap.put(new Text("moyCPUs"), new Text(datas[7]));
            
            context.write(new Text(datas[rowKey]), taskMap);
            
        }
    }
    
}