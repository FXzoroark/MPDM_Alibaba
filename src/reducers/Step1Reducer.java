package reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Step1Reducer extends Reducer<Text, MapWritable, Text, MapWritable> {

    private MultipleOutputs<Text, MapWritable> mos;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
        
        int instanceCount = 0;
        MapWritable resMap = new MapWritable();

        //liste des Jobs
        if(key.toString().startsWith("j")){
            Set<String> taches = new HashSet<>();
            Set<String> instances = new HashSet<>();

            for(MapWritable mapDatas: values){
                String tacheName = mapDatas.get(new Text("tacheName")).toString();
                String instanceName = mapDatas.get(new Text("tacheName")).toString();
                taches.add(tacheName);
                instances.add(instanceName);
            }
            resMap.put(new Text("nbTaches"), new LongWritable(taches.stream().count()));
            resMap.put(new Text("nbInstances"), new LongWritable(instances.stream().count()));
            mos.write("jobInfo", key, resMap);
        }
        //liste des taches
        else{
            int minTime = Integer.MAX_VALUE;
            int maxTime = 0;
    
            List<Integer> dureesInstances = new ArrayList<>();
    
            for(MapWritable mapDatas : values){
                String startTimeStr = mapDatas.get(new Text("startTime")).toString();
                String endTimeStr = mapDatas.get(new Text("endTime")).toString();
    
                instanceCount += 1;
    
                if(!startTimeStr.equals("") && !endTimeStr.equals("")){
                    int startTime = Integer.parseInt(startTimeStr);
                    int endTime = Integer.parseInt(endTimeStr);
                    if(endTime >= startTime) {
                        if(startTime < minTime){
                            minTime = startTime;
                        }
                        if(endTime > maxTime){
                            maxTime = endTime;
                        }
                        dureesInstances.add(endTime - startTime);
                    }
                    else {
                        dureesInstances.add(-1);
                    }
    
                }
                else{
                    dureesInstances.add(-1);
                }
    
            }
            //dans le cas ou une des deux valeurs sont manquante ou que les données ne sont pas cohérentes, la durée de la tache est à 0.
            int dureeTache = 0;
            if(maxTime >= minTime){
                dureeTache = maxTime - minTime;
            }
    
            //Calcule de la moyenne des données propres
            double meanWithoutMissing = dureesInstances.stream()
                                                       .filter(duree -> duree != -1)
                                                       .mapToInt(Integer::intValue)
                                                       .average()
                                                       .orElse(0.0);
            int meanCeil = (int) Math.ceil(meanWithoutMissing);
            //remplacement des données manquante par la moyenne
            dureesInstances.replaceAll(duree -> duree == -1 ? meanCeil : duree);
    
            double seuil = dureesInstances.stream()
                                          .mapToInt(Integer::intValue)
                                          .average()
                                          .orElse(0.0) * 1.2;
    
            long nbStragglers = dureesInstances.stream()
                                              .filter(duree -> duree > seuil)
                                              .count();
    
            resMap.put(new Text("dureeTache"), new IntWritable(dureeTache));
            resMap.put(new Text("nbInstances"), new IntWritable(instanceCount));
            resMap.put(new Text("nbStragglers"), new LongWritable(nbStragglers));
            
            mos.write("taskInfo", key, resMap);
        }
    }
}
