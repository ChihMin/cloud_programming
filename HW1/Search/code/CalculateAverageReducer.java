package calculateAverage;

import java.io.*;
import java.util.HashMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class CalculateAverageReducer extends Reducer<Text,SumCountPair,Text,SumCountPair> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
        HashMap<Integer, String> docName = new HashMap<Integer, String>(); 
        Configuration conf = context.getConfiguration();
        String[] keyArray = key.toString().split(" ");
        String inputPath = "HW1/input/" + keyArray[1];
        System.out.println("[REDUCER] " + inputPath);
        FileSystem fs = FileSystem.get(conf);

        for (SumCountPair data: values) {
            data.offsetParse(inputPath, fs);
            context.write(key, data);
        }
	}
}
