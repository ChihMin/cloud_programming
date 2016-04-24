package calculateAverage;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculateAverageMapper extends Mapper<LongWritable, Text, Text, SumCountPair> {
	
    private SumCountPair element ;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// we simply use StringTokenizer to split the words for us.
		/*
        StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			// TODO: find the first character of word, and create K,V pair for it.
			// we only need to handle Aa~Zz
			String toProcess = itr.nextToken();
            char chr = toProcess.charAt(0);
            if (isChar(chr)) {
                String chrInput = String.valueOf(chr);
                Text word = new Text();
                word.set(chrInput);
                context.write(word, one);
            }
		}
        */

        String[] stringArray = value.toString().split("\t");
	    String keyword = stringArray[0];
        int sum  = Integer.valueOf(stringArray[1]); 
        element = new SumCountPair(sum, 1);
        SumCountPair pair = element;
        System.out.println("[MAPPER] " + String.valueOf(pair.getSum()) + " " + String.valueOf(pair.getCount()));

        Text word = new Text();
        word.set(keyword);
        context.write(word, element);
    }
}
