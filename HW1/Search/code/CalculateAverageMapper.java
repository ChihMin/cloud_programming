package calculateAverage;

import java.io.*;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.Math;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CalculateAverageMapper extends Mapper<LongWritable, Text, Text, SumCountPair> {
	
    private SumCountPair element ;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// we simply use StringTokenizer to split the words for us.
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Configuration conf = context.getConfiguration();
        HashMap<String, ArrayList<Integer>> docHash;
        
        FileSystem fs = FileSystem.get(conf);
        /*
        String inputPath = conf.get("inputPath");
        BufferedReader br = new BufferedReader(
            new InputStreamReader(
                fs.open(new Path(inputPath + "/document_list.txt"))
            )
        );
        */
        String searchWord = conf.get("searchWord");
        if (!fileName.equals("document_list.txt")) {
            //System.out.println(value.toString());
            String line = value.toString();
            String[] docArray = line.split(";");
            String[] termArray;
            String word = docArray[0];
            double N = Double.valueOf(conf.get("documentNumber"));
            double df = Double.valueOf(word.split("\t")[1]);
            word = word.split("\t")[0];
            
            if (searchWord.equals(word)) {
                System.out.print(word + " " + String.valueOf(df) + " : ");
                for (int i = 1; i < docArray.length; ++i) {
                    String reg = "[.\\[\\],\\s]+";
                    termArray = docArray[i].split(reg);
                    String documentID = termArray[0];
                    double tf = Double.valueOf(termArray[1]);
                    double score = tf * Math.log10(N / df);
                    SumCountPair dataSet = new SumCountPair();
                    Text docID = new Text();
                    
                    System.out.print("(" + String.valueOf(documentID) + ", " + String.valueOf(tf) + ", " + String.valueOf(score) +  ") -> ~");
                    for (int j = 2; j < termArray.length; ++j) {
                        dataSet.pushData(Integer.valueOf(documentID), Integer.valueOf(termArray[j]));
                        System.out.print(termArray[j] + "->");
                    }
                    dataSet.setAnswer(conf.get(documentID));
                    docID.set(String.valueOf(score) + " " + conf.get(documentID));
                    context.write(docID, dataSet);
                    System.out.println("~");    
                }
            }
        }
    }
}
