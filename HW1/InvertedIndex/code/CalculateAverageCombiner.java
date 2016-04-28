package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageCombiner extends Reducer<Text,SumCountPair,Text,SumCountPair> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
        SumCountPair data = new SumCountPair(); 
        for (SumCountPair val: values) {
            val.pushFullData(data);    
        }

        context.write(key, data);
	}
}
