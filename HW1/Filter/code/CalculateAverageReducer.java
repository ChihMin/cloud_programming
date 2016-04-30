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
        SumCountPair data = new SumCountPair(); 
        for (SumCountPair val: values) {
            val.pushFullData(data);    
        }
        context.write(key, data);
	}
}
