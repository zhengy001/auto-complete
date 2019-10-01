package lm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LMReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
	private int topK;
	
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		topK = conf.getInt("k", 10);
	}
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
		for ( Text val : values ) {
			String input_val = val.toString();
			String word = input_val.split("\\|")[0];
			int cnt = Integer.parseInt(input_val.split("\\|")[1]);
			
			if ( tm.containsKey(cnt) ) {
				tm.get(cnt).add(word);
			} else {
				List<String> list = new ArrayList<String>();
				
				list.add(word);
				tm.put(cnt, list);
			}
			
			if ( tm.size() > topK ) {
				tm.pollLastEntry();
			}
		}
		
		Iterator<Integer> it = tm.keySet().iterator();
		while ( it.hasNext() ) {
			int keyCnt = (int) it.next();
			List<String> words = tm.get(keyCnt);
			for ( String word : words ) {
				context.write(new DBOutputWritable(key.toString(), word, keyCnt), NullWritable.get());
			}
		}
		
	}
}
