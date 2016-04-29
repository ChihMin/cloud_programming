package calculateAverage;

import java.io.*;
import java.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MyCustomRecordWriter extends RecordWriter<Text, SumCountPair> {
    private DataOutputStream out;
    private static int rank = 0;
    private static int currentNumber = 0;
    private static Double lastScore;

    public MyCustomRecordWriter(DataOutputStream stream) {
        out = stream;
        lastScore = Double.valueOf(-1);
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
        //close our file
        out.close();
    }

    @Override
    public void write(Text key, SumCountPair value) throws IOException, InterruptedException {
        //write out our key
        String[] keyArray = key.toString().split(" ");
        Double score = Double.valueOf(keyArray[0]);
        String fileName = keyArray[1];
        
        System.out.println("[WRITE] " + String.valueOf(keyArray[0]) + " " + keyArray[1]);
        
        if (!lastScore.equals(score)) {
            lastScore = Double.valueOf(score);
            rank = rank + 1;
        }
        
        if (currentNumber < 10)
            currentNumber++;
        else
            return;

        out.writeBytes("[Rank " + String.valueOf(rank) + "] ");
        out.writeBytes("Document name: " + fileName + "; ");
        out.writeBytes("Score: " + String.valueOf(score) + "\n");
        out.writeBytes("*************************\n");
        out.writeBytes(value.toString());
     }
}

