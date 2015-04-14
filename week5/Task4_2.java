import java.io.IOException;
import java.lang.String;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Random;

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
			extends Mapper<Object, Text, DoubleWritable, NullWritable> {
			
			private Random rands = new Random();
			private static final Double PERCENTAGE = 0.01;
			private static DoubleWritable sale = new DoubleWritable();

			public void map(Object key, Text value, Context context) throws
				IOException, InterruptedException {
				String[] fields = value.toString().split("\t");
				if (fields.length ==  6) {
					if (rands.nextDouble() < PERCENTAGE) {
						total += 1;
						sale.set((double)total);//Double.parseDouble(fields[4]));
						context.write(sale, NullWritable.get());
					}
				}
			}
	}

	public static class IntSumReducer extends Reducer<DoubleWritable, NullWritable, DoubleWritable, NullWritable> {

		private int cnt = 0;

		private DoubleWritable result = new DoubleWritable();

		public void reduce(DoubleWritable key, Iterable<NullWritable> values, Context context) throws
				IOException, InterruptedException {

			cnt += 1;
			//if (cnt == total / 2) {
				result.set(total);
				context.write(key, NullWritable.get());
		//	}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Random Sample");
		job.setJarByClass(Task4.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

