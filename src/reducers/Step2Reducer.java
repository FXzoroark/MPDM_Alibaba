package reducers;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import writables.Step2ArrayWritable;
import writables.Step2Writable;

public class Step2Reducer extends Reducer<Text, MapWritable, Text, Step2ArrayWritable> {

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
        

        int minTime = Integer.MAX_VALUE;
        int maxTime = 0;
        
        List<Integer> startTimes = new ArrayList<>();
        List<Integer> endTimes = new ArrayList<>();
        List<Integer> nbCores   = new ArrayList<>();

        int startTime = 0;
        int endTime = 0;
        int moyCPUs = 0;

        for(MapWritable mapDatas : values){
            String startTimeStr = mapDatas.get(new Text("startTime")).toString();
            String endTimeStr = mapDatas.get(new Text("endTime")).toString();
            String moyCPUsStr = mapDatas.get(new Text("moyCPUs")).toString();

            if(!startTimeStr.equals("") && !endTimeStr.equals("") && !moyCPUsStr.equals("")){
                startTime = Integer.parseInt(startTimeStr);
                endTime   = Integer.parseInt(endTimeStr);
                moyCPUs   = Integer.parseInt(moyCPUsStr);

                if(endTime >= startTime) {
                    if(startTime < minTime){
                        minTime = startTime;
                    }
                    if(endTime > maxTime){
                        maxTime = endTime;
                    }
                    startTimes.add(startTime);
                    endTimes.add(endTime);
                    nbCores.add((int) Math.ceil(moyCPUs/100.0));
                }

            }

        }
        if(maxTime == Integer.MAX_VALUE) return;

        int currSec = minTime;
        int nbCoresSumN1 = Integer.MAX_VALUE;

        int startingTimePic = -1;
        int dureePic = 1;

        ArrayList<Step2Writable> pics = new ArrayList<>();

        for(int i = minTime + 1; i < maxTime; i++){
            int nbCoresSumN2 = 0;

            for(int j = 0; j < startTimes.size(); j++){
                int instStart = startTimes.get(j);
                int instEnd   = endTimes.get(j);
                int instNbCores = nbCores.get(j);
                if (i >= instStart && i<= instEnd){
                    nbCoresSumN2 += instNbCores;
                }
            }

            if(nbCoresSumN2 > nbCoresSumN1){
                startingTimePic = i;
            }
            else if (startingTimePic != -1 ){
                if(nbCoresSumN2 < nbCoresSumN1){
                    pics.add(new Step2Writable(startingTimePic, nbCoresSumN1, dureePic));
                    startingTimePic = -1;
                    dureePic = 1;
                }
                else{
                    dureePic += 1;
                }
            }
            nbCoresSumN1 = nbCoresSumN2;
        }

        //si la tache ce fini sur un pic
        if(startingTimePic != -1){
            pics.add(new Step2Writable(startingTimePic, nbCoresSumN1, dureePic));
        }

        Collections.sort(pics);

        context.write(key, new Step2ArrayWritable(pics.toArray(new Step2Writable[0])));


    }
    
}
