package lm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LMMapper extends Mapper<LongWritable, Text, Text, Text> {
	private int threshold;
	
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		threshold = conf.getInt("threshold", 5);
	}
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if ( value == null )
			return;
		
		String line = value.toString().trim();
		if ( line.length() == 0 ) {
			return;
		}
		
		String[] grams = line.split("\t");
		if ( grams.length < 2 )
			return;
		
		String[] words = grams[0].split("\\s+");
		int cnt = Integer.valueOf(grams[grams.length-1]);
		
		// filter out count < threshold
		if ( cnt <= threshold ) {
			return;
		}
		
		StringBuilder sb = new StringBuilder();
		for ( int i = 0; i < words.length-1; i++ ) {
			sb.append(words[i]);
			sb.append(" ");
		}
		
		String outputKey = sb.toString().trim();
		if ( outputKey == null || outputKey.length() < 1 ) {
			return;
		}

		Text outputValue = new Text(words[words.length-1]+"|"+cnt);
		context.write(new Text(outputKey), outputValue);
	}
}
