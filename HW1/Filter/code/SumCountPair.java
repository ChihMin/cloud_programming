package calculateAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class SumCountPair implements Writable {
    private String answer;
    private ArrayList<Integer> docList;
    private HashMap<Integer, ArrayList<Integer>> docHash;
    private HashMap<Integer, ArrayList<String>> docNameHash; 
	private int sum, count;

	public SumCountPair() {
		docList = new ArrayList<Integer>();
        docHash = new HashMap<Integer, ArrayList<Integer>>();
	}
	
	public SumCountPair(int sum, int count) {
		//TODO: constructor
        this.sum = sum;
        this.count = count;
	}

	@Override 
	public void write(DataOutput out) throws IOException {
        int docNumber = docList.size();
        out.writeInt(docNumber);
        for (int i = 0; i < docNumber; ++i) {
            Integer documentID = docList.get(i);
            out.writeInt(documentID); 
            
            ArrayList<Integer> patternList = docHash.get(documentID);
            out.writeInt(patternList.size());
            for (Integer offset : patternList) {
                out.writeInt(offset);
            }
        }
    }

	@Override
	public void readFields(DataInput in) throws IOException {
        this.docList = new ArrayList<Integer>();
        this.docHash = new HashMap<Integer, ArrayList<Integer>>();
        Integer docNumber = in.readInt();
        for (int i = 0; i < docNumber; ++i) {
            Integer documentID = in.readInt();
            ArrayList<Integer> patternList = this.docHash.get(documentID);
            if (patternList == null) {
                patternList = new ArrayList<Integer>();
                this.docList.add(documentID);
                this.docHash.put(documentID, patternList);
            }
            int patternSize = in.readInt();
            for (int j = 0; j < patternSize; ++j) {
                Integer offset = in.readInt();
                patternList.add(offset);
            }
        }
    }
    
    public void pushData(Integer documentID, Integer offset) {
        ArrayList<Integer> patternList;
        if ((patternList = docHash.get(documentID)) == null) {
            patternList = new ArrayList<Integer>();
            docList.add(documentID);
            docHash.put(documentID, patternList);
        }
        patternList.add(offset);
    }
    
    public void setAnswer(String ans) {
        this.answer = ans;
    }
    	
	public int getSum() {
		return sum;
	}
	
	public int getCount() {
		return count;
	}

    
    public void pushFullData(SumCountPair dataSet) {
        for (Integer documentID: docList) {
            ArrayList<Integer> patternList = docHash.get(documentID);
            for (Integer offset: patternList) {
                dataSet.pushData(documentID, offset);
            }
        }
    }
    
    public void offsetParse(String inputPath, FileSystem fs) throws IOException, InterruptedException{
        System.out.println("[offsetParse]");
        this.docNameHash = new HashMap<Integer, ArrayList<String>>();
        for (Integer documentID : docList) {
            ArrayList<Integer> offsetList = docHash.get(documentID);
            ArrayList<String> termList = new ArrayList<String>();
            for (Integer offset: offsetList) { 
                BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                        fs.open(new Path(inputPath)), "UTF8" 
                    )
                );

                String termSeq;
                br.skip(offset);
                termSeq = br.readLine();
                br.close();
                termList.add(termSeq);
                System.out.println(termSeq + ", ");
            }
            System.out.println("[PARSE] " + termList.toString());
            docNameHash.put(documentID, termList);
        }
    }

    @Override
    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append(String.valueOf(docList.size()) + ";");
        for (Integer documentID : docList) {
            ArrayList<Integer> patternList = docHash.get(documentID);
            str.append(String.valueOf(documentID) + 
                " " + String.valueOf(patternList.size()));
            str.append(patternList.toString());
            str.append(";");
        }
        return str.toString();
    }
	
}
