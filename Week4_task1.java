import java.io.IOException;
import java.lang.String;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Week4_task1 {
	public static class TokenizerMapper
			extends Mapper<Object, Text, Text, IntWritable> {


			private String getURL(String str) {
					int startindex = -1, endindex = -1;
					startindex = str.indexOf("GET");
					if (startindex < 0) {
						startindex = str.indexOf("POST");
						if (startindex < 0) return null;
					}
					endindex = str.indexOf(" HTTP/");
					if (endindex < 0) return null;
					return str.substring(startindex, endindex);			
			}

			private Text word = new Text();
			private final static IntWritable one = new IntWritable(1);
			private HashMap<String, Boolean> hashmap = new HashMap<String, Boolean>();

			public void map(Object key, Text value, Context context) throws
				IOException, InterruptedException {
					String url = getURL(value.toString());
					if (url == null) return;
					String ip = value.toString().split(" ")[0];
					if (hashmap.containsKey(url+ip) == false) {
						hashmap.put(url+ip, true);
						word.set(url);
						context.write(word, one);
					}
			}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
				IOException, InterruptedException {
				int sum = 0;
				for (IntWritable val : values) {
					sum = sum + val.get();
				}
				result.set(sum);
				context.write(key, result);
			}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "URL Rate");
		job.setJarByClass(Week4_task1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

