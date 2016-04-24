package calculateAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SumCountPair implements Writable {

	private int sum;
	private int count;

	public SumCountPair() {
		
	}
	
	public SumCountPair(int sum, int count) {
		//TODO: constructor
        this.sum = sum;
        this.count = count;
	}

	@Override 
	public void write(DataOutput out) throws IOException {
  //      out.writeDouble(Double.valueOf(this.sum) / Double.valueOf(this.sum));
        out.writeInt(sum);
        out.writeInt(count);
    }

	@Override
	public void readFields(DataInput in) throws IOException {
        this.sum = in.readInt();
        this.count = in.readInt();
	}
	
	public int getSum() {
		return sum;
	}
	
	public int getCount() {
		return count;
	}
/*
    @Override
    public String toString() {
        double dSum = Double.valueOf(this.sum);
        double dCount = Double.valueOf(this.count);
        return String.valueOf(dSum);
    }
*/	
}
