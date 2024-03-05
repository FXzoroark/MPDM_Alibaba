package drivers;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mappers.TacheMapper;
import reducers.TacheReducer;

public class Step1 extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 2){
            System.out.printf("Usage: Step1 <input dir> <output dir>");
            return -1;
        }

        Job job = Job.getInstance(this.getConf());

        job.setJarByClass(Step1.class);
        job.setJobName("Step1");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TacheMapper.class);
        job.setReducerClass(TacheReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        boolean success = job.waitForCompletion(true);

        if(success){
            return 0;
        }
        else{
            return 1;
        }
        
        
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Step1(), args);
        System.exit(exitCode);
    }
}
