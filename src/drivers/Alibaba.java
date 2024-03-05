package drivers;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mappers.Step1Mapper;
import mappers.Step2Mapper;
import mappers.Step3Mapper;
import reducers.Step1Reducer;
import reducers.Step2Reducer;
import reducers.Step3Reducer;
import writables.Step2ArrayWritable;

public class Alibaba extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length < 2){
            System.out.printf("Usage: Step1 <input dir> <output dir> [generic options]");
            return -1;
        }

		Configuration conf = this.getConf() ; 
        
        int i = 2;
        while(i<args.length){
			if(args[i].equals("-noTerminated")){
				conf.setBoolean("getTerminated",false);
			}
            else if (args[i].equals("-noFailed")){
                conf.setBoolean("getFailed", false);
            }
           else{
				System.out.printf("Unknown option "+args[2]+"\n");
				System.exit(-1); 					
			}
			i++ ;
		}

        Job jobStep1 = Job.getInstance(conf);

        jobStep1.setJarByClass(Alibaba.class);
        jobStep1.setJobName("Step1");

        FileInputFormat.setInputPaths(jobStep1, new Path(args[0]));

        MultipleOutputs.addNamedOutput(jobStep1, "taskInfo", TextOutputFormat.class, Text.class, MapWritable.class);
        MultipleOutputs.addNamedOutput(jobStep1, "jobInfo", TextOutputFormat.class, Text.class, MapWritable.class);

        TextOutputFormat.setOutputPath(jobStep1, new Path(args[1]+"/step1"));

        jobStep1.setMapperClass(Step1Mapper.class);
        jobStep1.setReducerClass(Step1Reducer.class);
        
        jobStep1.setMapOutputKeyClass(Text.class);
        jobStep1.setMapOutputValueClass(MapWritable.class);

        jobStep1.setOutputKeyClass(Text.class);
        jobStep1.setOutputValueClass(ArrayWritable.class);

        boolean succ1 = jobStep1.waitForCompletion(true);

        conf.set("rowKey", "1");
        Job jobStep2 = Job.getInstance(conf);

        jobStep2.setJarByClass(Alibaba.class);
        jobStep2.setJobName("Step2");
        
        FileInputFormat.setInputPaths(jobStep2, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobStep2, new Path(args[1]+ "/step2"));

        jobStep2.setMapperClass(Step2Mapper.class);
        jobStep2.setReducerClass(Step2Reducer.class);
        
        jobStep2.setMapOutputKeyClass(Text.class);
        jobStep2.setMapOutputValueClass(MapWritable.class);

        jobStep2.setOutputKeyClass(Text.class);
        jobStep2.setOutputValueClass(Step2ArrayWritable.class);

        boolean succ2 = jobStep2.waitForCompletion(true);

        //Job nécessaire au préalable pour l'étape 3
        conf.set("rowKey", "6");
        Job jobPreStep3 = Job.getInstance(conf);

        jobPreStep3.setJarByClass(Alibaba.class);
        jobPreStep3.setJobName("preStep3");
        
        FileInputFormat.setInputPaths(jobPreStep3, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobPreStep3, new Path(args[1]+ "/preStep3"));

        jobPreStep3.setMapperClass(Step2Mapper.class);
        jobPreStep3.setReducerClass(Step2Reducer.class);
        
        jobPreStep3.setMapOutputKeyClass(Text.class);
        jobPreStep3.setMapOutputValueClass(MapWritable.class);

        jobPreStep3.setOutputKeyClass(Text.class);
        jobPreStep3.setOutputValueClass(Step2ArrayWritable.class);

        boolean succPre3 = jobPreStep3.waitForCompletion(true);

        boolean succ3 = false;
        //nous dépendons du jobPreStep3 pour le job3
        if(succPre3){
            Job jobStep3 = Job.getInstance(conf);

            jobStep3.setJarByClass(Alibaba.class);
            jobStep3.setJobName("Step3");
            
            FileInputFormat.addInputPath(jobStep3, new Path(args[1] + "/preStep3/part-r-00000"));
            FileInputFormat.addInputPath(jobStep3, new Path(args[1] + "/preStep3/part-r-00001"));
            FileOutputFormat.setOutputPath(jobStep3, new Path(args[1] + "/step3"));

            jobStep3.setMapperClass(Step3Mapper.class);
            jobStep3.setReducerClass(Step3Reducer.class);
            
            jobStep3.setMapOutputKeyClass(Text.class);
            jobStep3.setMapOutputValueClass(IntWritable.class);

            jobStep3.setOutputKeyClass(Text.class);
            jobStep3.setOutputValueClass(IntWritable.class);
            
            succ3 = jobStep3.waitForCompletion(true);
        }




        boolean success = succ1 && succ2 && succ3;

        if(success){
            return 0;
        }
        else{
            return 1;
        }
        
        
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Alibaba(), args);
        System.exit(exitCode);
    }
}
