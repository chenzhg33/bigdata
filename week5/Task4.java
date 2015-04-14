import java.io.IOException;
import java.lang.String;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Random;
import java.util.Collections;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task4 {

	private static int total = 0;

	public static class TokenizerMapper
			extends Mapper<Object, Text, Text, DoubleWritable> {
			
			private Random rands = new Random();
			private static final Double PERCENTAGE = 0.01;
			private static DoubleWritable sale = new DoubleWritable();
			private static Text head = new Text("median:");

			public void map(Object key, Text value, Context context) throws
				IOException, InterruptedException {
				String[] fields = value.toString().split("\t");
				if (fields.length ==  6) {
					if (rands.nextDouble() < PERCENTAGE) {
						sale.set(Double.parseDouble(fields[4]));
						context.write(head, sale);
					}
				}
			}
	}

	public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {


		private DoubleWritable result = new DoubleWritable();
		private ArrayList<Double> list = new ArrayList<Double>();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws
				IOException, InterruptedException {

				for (DoubleWritable val : values) {
					list.add(val.get());
				}
				int len = list.size();
				Collections.sort(list);
				result.set(list.get(len/2));
				context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Random Sample");
		job.setJarByClass(Task4.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

