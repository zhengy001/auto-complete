package ngram;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private int noGram;
	private final IntWritable one = new IntWritable(1);
	
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		noGram = conf.getInt("noGram", 3);
	}
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		line = line.trim().toLowerCase();
		line = line.replaceAll("[^a-z]+", " ");
		String[] words = line.split("\\s+");
		
		if ( words.length < 2 ) {
			return;
		}
		
		StringBuilder sb;
		for ( int i = 0; i < words.length-1; i++ ) {
			sb = new StringBuilder();
			// ignore one gram
			for ( int j = 1; i+j < words.length && j < noGram; j++ ) {
				sb.append(" ");
				sb.append(words[i+j]);
				context.write(new Text(sb.toString().trim()), one);
			}
		}
	}
}
