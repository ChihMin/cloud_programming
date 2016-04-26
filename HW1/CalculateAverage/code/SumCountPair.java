package calculateAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;

public class SumCountPair implements Writable {

    private ArrayList<Integer> docList;
    private HashMap<Integer, ArrayList<Integer>> docHash; 
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
    	
	public int getSum() {
		return sum;
	}
	
	public int getCount() {
		return count;
	}

    @Override
    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append(String.valueOf(docList.size()) + ";");
        for (Integer documentID : docList) {
            ArrayList<Integer> patternList = docHash.get(documentID);
            str.append(String.valueOf(documentID) + 
                " " + String.valueOf(patternList.size()) + "[");
            for (Integer offset : patternList) {
                str.append(String.valueOf(offset) + ", ");
            }
            str.append("];");
        }
        return str.toString();
    }
	
}
