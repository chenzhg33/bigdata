
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task4 {

	private static int total = 0;

	public static class TokenizerMapper
			extends Mapper<Object, Text, DoubleWritable, NullWritable> {

			private Random rands = new Random();
			private static final Double PERCENTAGE = 0.1;
			private static DoubleWritable sale = new DoubleWritable();

			public void map(Object key, Text value, Context context) throws
				IOException, InterruptedException {
				if (rands.nextDouble() < PERCENTAGE) {
					total += 1;
					sale.set(Double.parseDouble(value.toString()));
					context.write(sale, NullWritable.get());
				}
			}
	}

	public static class MyReducer
			extends Reducer<DoubleWritable, NullWritable, DoubleWritable, NullWritable> {

		private int cnt = 0;

		public void reduce(DoubleWritable key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
			if (cnt == total / 2) {
				context.write(key, NullWritable.get());
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Counter");
		job.setJarByClass(Task4.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
