package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable> {
	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		// customize which <K ,V> will go to which reducer
        char chr = key.toString().charAt(0);
        if (chr >= 'a' && chr <= 'g')
            return 0;
        else if (chr >= 'A' && chr <= 'G')
            return 0;
        else
            return 1;

	}
}
