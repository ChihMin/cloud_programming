package calculateAverage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import java.io.*;

public class CalculateAverage {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(args[0]));
        StringBuffer fileString = new StringBuffer();

        int index = 0;
        for (FileStatus file : files) {
            String fileName = file.getPath().getName();
            conf.set(fileName, String.valueOf(++index));
        }
		conf.set("fileNumber", String.valueOf(index));

		Job job = Job.getInstance(conf, "CalculateAverage");
		job.setJarByClass(CalculateAverage.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(CalculateAverageMapper.class);
		job.setCombinerClass(CalculateAverageCombiner.class);
		//job.setPartitionerClass(CalculateAveragePartitioner.class);
		//job.setSortComparatorClass(xxx.class);
		job.setReducerClass(CalculateAverageReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SumCountPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SumCountPair.class);
		
		// set the number of reducer
		job.setNumReduceTasks(1);
		
        // set global varialbe for hadoop

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
        int hasError = job.waitForCompletion(true) ? 0 : 1;	
        
        BufferedWriter bw = new BufferedWriter(
            new OutputStreamWriter(
                fs.create(new Path(args[1] + "/document_list.txt"), true)
            )
        );
        index = 0;
        for (FileStatus file : files) {
            String fileName = file.getPath().getName();
            String line = String.valueOf(String.valueOf(++index) + " " + fileName);
            bw.write(line + "\n");
        }
        bw.close();
        System.exit(hasError);
	}
}
