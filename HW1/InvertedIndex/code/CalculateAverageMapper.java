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
        System.out.println("[ORIGIN STRING] " + value.toString());
        System.out.println("[File name] " + fileName + " -> [index] " + indexStr);
        System.out.println("[CODE POINT] " + String.valueOf(key.toString()));
        System.out.println("[FILE NUMBER] " + conf.get("fileNumber"));

        String[] strArray = value.toString().split("[^a-zA-Z]+");
        int fromIndex = 0;
        int documentID = Integer.valueOf(indexStr);
        for (String str : strArray) {
            if (str.length() != 0) {
                SumCountPair dataSet = new SumCountPair();
                fromIndex = value.toString().indexOf(str, fromIndex);
                int offset = Integer.valueOf(key.toString()) + fromIndex;
                fromIndex += str.length();
                
                dataSet.pushData(documentID, offset);
                Text word = new Text();
                word.set(str);
                context.write(word, dataSet);
                System.out.println("[MAPPER] " + dataSet.toString()); 
                // System.out.println("[Mapper] " + fileName + " -> [Offset = " + String.valueOf(offset) + "] -> " + str); 
            }
        }
        
        //int sum  = Integer.valueOf(stringArray[1]); 

    }
}
