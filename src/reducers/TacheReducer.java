package reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.Context;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TacheReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    
    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
        int instanceCount = 0;
        int minTime = Integer.MAX_VALUE;
        int maxTime = 0;

        List<Integer> dureesInstances = new ArrayList<>();

        for(MapWritable mapDatas : values){
            String startTimeStr = mapDatas.get(new Text("startTime")).toString();
            String endTimeStr = mapDatas.get(new Text("endTime")).toString();

            int startTime = 0;
            int endTime = 0;

            instanceCount += 1;

            if(!startTimeStr.equals("")){
                startTime = Integer.parseInt(startTimeStr);
            }
            if(!endTimeStr.equals("")){
                endTime = Integer.parseInt(endTimeStr);
            }

            dureesInstances.add(endTime - startTime);

            if(startTime < minTime){
                minTime = startTime;
            }

            if(endTime > maxTime){
                maxTime = endTime;
            }
        }

        int dureeTache = maxTime - minTime;

        double seuil = dureesInstances.stream()
                                      .mapToInt(Integer::intValue)
                                      .average()
                                      .orElse(0.0) * 1.2;

        long nbStragglers = dureesInstances.stream()
                                          .filter(duree -> duree > seuil)
                                          .count();

        MapWritable resMap = new MapWritable();
        resMap.put(new Text("dureeTache"), new IntWritable(dureeTache));
        resMap.put(new Text("nbInstances"), new IntWritable(instanceCount));
        resMap.put(new Text("nbStragglers"), new LongWritable(nbStragglers));

        context.write(key, resMap);
    }
}
