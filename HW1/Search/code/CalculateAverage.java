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
        BufferedReader br = new BufferedReader(
            new InputStreamReader(
                fs.open(new Path(args[0] + "/document_list.txt"))
            )
        );
        
        int documentNumber = 0; 
        String line;
        line = br.readLine();
        while (line != null) {
            //System.out.println(line);
            String[] docKeyValue = line.split(" ");
            conf.set(docKeyValue[0], docKeyValue[1]);
            System.out.println(docKeyValue[0] + " " + docKeyValue[1]);
            line = br.readLine();
            documentNumber++;
        }
        br.close();
        
        conf.set("searchWord", args[2]);
        conf.set("inputPath", args[0]);
        conf.set("outputPath", args[1]);
        conf.set("documentNumber", String.valueOf(documentNumber));
/*
        int index = 0;
        for (FileStatus file : files) {
            String fileName = file.getPath().getName();
            conf.set(fileName, String.valueOf(++index));
        }
		conf.set("fileNumber", String.valueOf(index));
*/
		Job job = Job.getInstance(conf, "CalculateAverage");
		job.setJarByClass(CalculateAverage.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(CalculateAverageMapper.class);
		//job.setCombinerClass(CalculateAverageCombiner.class);
		//job.setPartitionerClass(CalculateAveragePartitioner.class);
		job.setSortComparatorClass(CalculateAverageComparator.class);
		job.setReducerClass(CalculateAverageReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SumCountPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SumCountPair.class);
        job.setOutputFormatClass(MyTextOutputFormat.class);	    
	
		// set the number of reducer
		job.setNumReduceTasks(1);
		
        // set global varialbe for hadoop

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
