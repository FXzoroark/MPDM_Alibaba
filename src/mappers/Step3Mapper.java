package mappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Step3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] datas = line.split("\t");

        //première ligne donc pic le plus haut
        if (datas.length == 4){
            context.write(new Text(datas[0]), new IntWritable(Integer.parseInt(datas[2])));
        }
    }
    
}
