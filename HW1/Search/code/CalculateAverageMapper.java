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
        
        
        if (!fileName.equals("document_list.txt")) {
            //System.out.println(value.toString());
            String line = value.toString();
            String[] docArray = line.split(";");
            String[] termArray;
            String word = docArray[0];
            int df = Integer.valueOf(word.split("\t")[1]);
            int tf;
            word = word.split("\t")[0];
            
            System.out.print(word + " " + String.valueOf(df) + " : ");
            for (int i = 1; i < docArray.length; ++i) {
                String reg = "[.\\[\\],\\s]+";
                termArray = docArray[i].split(reg);
                tf = Integer.valueOf(termArray[1]);
                System.out.print(String.valueOf(tf) + " -> ~");
                for (int j = 2; j < termArray.length; ++j)
                    System.out.print(termArray[j] + ", ");
                System.out.println("~");    
            }

            
            //System.out.println(word + " " + String.valueOf(df));
        }
/*
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
            }
        }
*/  
    }
}
