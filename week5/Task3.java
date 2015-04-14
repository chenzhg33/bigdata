
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task3 {
	public static class TokenizerMapper
			extends Mapper<Object, Text, NullWritable, NullWritable> {

			public static final String SHOP = "SHOP";
			public static final String SHOP1 = "Las Vegas";
			public static final String SHOP2 = "Arlington";
			
			public void map(Object key, Text value, Context context) throws
				IOException, InterruptedException {
				String[] fields = value.toString().split("\t");
				if (fields.length ==  6) {
						if (fields[2].equals(SHOP1)) {
								context.getCounter(SHOP, SHOP1).increment((int)(Double.parseDouble(fields[4]) * 1000));
						} else if (fields[2].equals(SHOP2)) {
								context.getCounter(SHOP, SHOP2).increment((int)(Double.parseDouble(fields[4]) * 1000));
						}
				}
			}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Counter");
		job.setJarByClass(Task3.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int code = job.waitForCompletion(true) ? 0 : 1;
		if (code == 0) {
			// Job successfully processed, retrieve counters
			for (Counter counter : job.getCounters().getGroup(
				"SHOP")) {
				System.out.println(counter.getDisplayName() + "\t"
						+ (float)counter.getValue() / 1000);
			}
		}
	}
}
