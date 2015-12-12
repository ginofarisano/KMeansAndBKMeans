package org.soa.cenrandom;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class CombinerCentroids extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	private Random rnd = new Random();
	private int prob;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		this.prob = conf.getInt("prob", 4) / 4;
		if (this.prob < 1)
			this.prob = 2;
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int t, i = 0, sended = 0;
		Text[] bk = new Text[2];

		for (Text doc : values) {

			t = this.rnd.nextInt(this.prob);
			if (this.rnd.nextInt(this.prob) == t) {
				sended++;
				context.write(key, doc);
			} else {

				if (i < 2) {
					bk[i] = new Text(doc);
					// System.out.println("aaa " + teee[i]);
					i++;
				}
			}

		}

		// se ho mandato scritto meno di due centroidi
		for (t = sended; t < 2; t++) {
			// System.out.println("eee " + teee[t]);
			context.write(key, bk[t]);
		}

	}

}
