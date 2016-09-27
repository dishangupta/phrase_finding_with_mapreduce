import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/** A WritableComparable for ints. */
public class CountFormatWritable implements Writable {

	private long Bx;
	private long Cx;
	private long Bxy;
	private long Cxy;
	private long By;
	private long Cy;
			
	
	public CountFormatWritable (){};	
	
	public CountFormatWritable(long Bx, long Cx, long Bxy, long Cxy,
				long By, long Cy) { 
		setBx(Bx); 
		setCx(Cx); 
		setBxy(Bxy); 
		setCxy(Cxy); 
		setBy(By); 
		setCy(Cy); 
		
	}

	public void setBx(long Bx) { 
		this.Bx = Bx; 
	}

	public void setCx(long Cx) { 
		this.Cx = Cx; 
	}

	public void setBxy(long Bxy) { 
		this.Bxy = Bxy; 
	}

	public void setCxy(long Cxy) { 
		this.Cxy = Cxy; 
	}
	
	public void setBy(long By) { 
		this.By = By; 
	}

	public void setCy(long Cy) { 
		this.Cy = Cy; 
	}
	
	public long getBx() { 
		return Bx; 
	}

	public long getCx() { 
		return Cx; 
	}

	public long getBxy() { 
		return Bxy; 
	}

	public long getCxy() { 
		return Cxy; 
	}

	public long getBy() { 
		return By; 
	}

	public long getCy() { 
		return Cy; 
	}

	@Override
	public void readFields(DataInput in) throws IOException {	

   		Bx = in.readLong();
		Cx = in.readLong();
		Bxy = in.readLong();
		Cxy = in.readLong();
		By = in.readLong();
		Cy = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(Bx);
		out.writeLong(Cx);
		out.writeLong(Bxy);
		out.writeLong(Cxy);
		out.writeLong(By);
		out.writeLong(Cy);
	}


	public String toString() {

		return "Bx="+Bx+"Cx="+Cx+"Bxy="+Bxy+"Cxy="+Cxy+"By="+By+"Cy="+Cy;

	}

}

