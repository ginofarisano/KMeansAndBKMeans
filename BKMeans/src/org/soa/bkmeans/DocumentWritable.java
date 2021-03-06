package org.soa.bkmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

public class DocumentWritable implements Writable {

	private MapWritable parole = new MapWritable();
	private IntWritable docsNum = new IntWritable(0);
	
	
	public MapWritable getParole() {
		return parole;
	}

	public void setParole(MapWritable parole) {
		this.parole = parole;
		this.docsNum.set(1);
	}

	public IntWritable getDocsNum() {
		return docsNum;
	}

	public void setDocsNum(IntWritable docsNum) {
		this.docsNum = docsNum;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.parole.readFields(arg0);
		this.docsNum.readFields(arg0);

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.parole.write(arg0);
		this.docsNum.write(arg0);

	}
	
public void merge(DocumentWritable doc){
		
		Iterator<Entry<Writable, Writable>> iter = doc.getParole().entrySet().iterator();

		double value=0;
		double temp=0;
		
		DoubleWritable ZERO = new DoubleWritable(0);
		

		while(iter.hasNext()) {
			Entry<Writable, Writable> t = iter.next();
			value = ((DoubleWritable)t.getValue()).get();
			//the intermediate vector has the word in the toMerge vector?
			temp = ((DoubleWritable) this.parole.getOrDefault(t.getKey(), ZERO)).get(); 
			//modify or add the value
			value += temp;
			this.parole.put(new Text((Text) t.getKey()),new DoubleWritable(value));
		}
		
		//incremento il contatore
		this.docsNum.set(this.docsNum.get()+doc.getDocsNum().get());
			
	}

}
