package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// we simply use StringTokenizer to split the words for us.
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
		
	}

    private boolean isChar(char chr) {
        if (chr >= 'a' && chr <= 'z')
            return true;
        else if (chr >= 'A' && chr <= 'Z')
            return true;
        else
            return false;
    }
	
}
