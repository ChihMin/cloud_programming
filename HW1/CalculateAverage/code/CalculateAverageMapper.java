package calculateAverage;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;


public class CalculateAverageMapper extends Mapper<LongWritable, Text, Text, SumCountPair> {
	
    private SumCountPair element ;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// we simply use StringTokenizer to split the words for us.
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Configuration conf = context.getConfiguration();
        String indexStr = conf.get(fileName);
        System.out.println("[File name] " + fileName + " -> [index] " + indexStr);
        System.out.println("[CODE POINT] " + String.valueOf(key.toString()));

        String[] strArray = value.toString().split("[^a-zA-Z]+");
        for (String str : strArray) {
            if (str.length() != 0)
                System.out.println("[Mapper] " + fileName + " -> [ID = " + indexStr + "] -> " + str); 
        }
        
        // String[] stringArray = value.toString().split("\t");
	    String keyword = "123";
        //int sum  = Integer.valueOf(stringArray[1]); 
        int sum  = Integer.valueOf("1"); 
        element = new SumCountPair(sum, 1);
        SumCountPair pair = element;

        Text word = new Text();
        word.set(keyword);
        context.write(word, element);
    }
}
