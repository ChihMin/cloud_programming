package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageReducer extends Reducer<Text,SumCountPair,Text,SumCountPair> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
        
        for (SumCountPair val: values) {
            context.write(key, val);
            System.out.println("[REDUCER] " + val.toString());
		}
        //SumCountPair pair = new SumCountPair(sum, count);
        //context.write(key, pair);
	}
}
