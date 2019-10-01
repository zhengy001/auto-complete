package auto;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import lm.DBOutputWritable;
import lm.LMMapper;
import lm.LMReducer;
import ngram.NgramMapper;
import ngram.NgramReducer;

public class Driver {
	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Options options = new Options();

        Option input = new Option("i", true, "input file path");
        input.setRequired(true);
        
        Option output = new Option("o", true, "output file");
        output.setRequired(true);
        
        options.addOption(input);
        options.addOption(output);
        options.addOption(new Option("n", true, "ngram\t(default: 5)"));
        options.addOption(new Option("t", true, "threshold to drop\t(default: 5)"));
        options.addOption(new Option("k", true, "top k\t(default: 10)"));

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            
            if (buildNgram(cmd) == 1 ) {
				throw new RuntimeException("Error: Ngram Build Failed");
			}
            
            if (buildLanguageModel(cmd) == 1) {
				throw new RuntimeException("Error: Language Model Build Failed");
			}
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp(Driver.class.getName(), options);

            System.exit(1);
        } catch (RuntimeException e) {
        	System.out.println(e.getMessage());
        	
        	System.exit(1);
        }
	}
	
	private static int buildNgram(CommandLine cmd)
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", ".");
		conf.set("noGram", cmd.getOptionValue("n"));
		
		Job job = Job.getInstance(conf);
		
		job.setJobName("Ngram");
		job.setJarByClass(Driver.class);
		job.setMapperClass(NgramMapper.class);
		job.setReducerClass(NgramReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(cmd.getOptionValue("i")));
		FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("o")));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private static int buildLanguageModel(CommandLine cmd)
			throws IOException, ClassNotFoundException, InterruptedException {
		
		final String[] fields = {"starting_phrase", "following_word", "cnt"};
		
		Configuration conf = new Configuration();
		conf.set("threshold", cmd.getOptionValue("t"));
		conf.set("topK", cmd.getOptionValue("k"));
		
		// Set before getting job instance
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
		          "jdbc:mysql://192.168.0.19:8889/hadoop_ngram", "root", "root");
		
		Job job = Job.getInstance(conf);
		
		job.setJobName("LM");
		job.setJarByClass(Driver.class);
		job.setMapperClass(LMMapper.class);
		job.setReducerClass(LMReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(DBOutputWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(DBOutputFormat.class);
		
		job.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.43.jar"));
		
		FileInputFormat.setInputPaths(job, new Path(cmd.getOptionValue("o")));
		// Output into database "output" table
		DBOutputFormat.setOutput(job, "output", fields);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
