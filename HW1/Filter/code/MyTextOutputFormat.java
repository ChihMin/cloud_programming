package calculateAverage;

import java.io.*;
import java.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.*;

public class MyTextOutputFormat extends FileOutputFormat<Text, SumCountPair> {
  @Override
  public org.apache.hadoop.mapreduce.RecordWriter<Text, SumCountPair> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
     //get the current path
     Path path = FileOutputFormat.getOutputPath(arg0);
     //create the full path with the output directory plus our filename
     Path fullPath = new Path(path, "output_retrieval.txt");

     //create the file in the file system
     FileSystem fs = path.getFileSystem(arg0.getConfiguration());
     FSDataOutputStream fileOut = fs.create(fullPath, arg0);

     //create our record writer with the new file
     return new MyCustomRecordWriter(fileOut);
  }
}

