import java.io.IOException;
import java.lang.String;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Week4_task2 {
	public static class TokenizerMapper
			extends Mapper<Object, Text, Text, IntWritable> {
			
			private static String getTime(String str) {
				String regex = "\\d{2}/\\w{3}/\\d{4}:\\d{2}";
				Pattern pattern = Pattern.compile(regex);
				Matcher match = pattern.matcher(str);
				match.find();
				return match.group();
			}

			private static HashMap<String, Integer> monthMap = new HashMap<String, Integer>();
			private void init() {
				monthMap.put("Jan", 1);
				monthMap.put("Feb", 2);
				monthMap.put("Mar", 3);
				monthMap.put("Apr", 4);
				monthMap.put("May", 5);
				monthMap.put("Jun", 6);
				monthMap.put("Jul", 7);
				monthMap.put("Aug", 8);
				monthMap.put("Sep", 9);
				monthMap.put("Oct", 10);
				monthMap.put("Nov", 11);
				monthMap.put("Dec", 12);
			}

			// to judge whether the date is between 2009/7/1 and 2010/8/1	
			private Boolean isDateValid(String str) {
				init();
				String[] fields = str.split("/");
				int day = Integer.parseInt(fields[0]);
				int month = monthMap.get(fields[1]);
				int year = Integer.parseInt(fields[2].split(":")[0]);
				int hour = Integer.parseInt(fields[2].split(":")[1]);
				if (year < 2009) return false;
				if (year == 2009 && month < 7) return false;
				if (year > 2010) return false;
				if (year == 2010) {
					if (month > 8) return false;
					if (month == 8 && day > 1) return false;
				}
				return true;
			}

			private Text word = new Text();
			private final static IntWritable one = new IntWritable(1);

			public void map(Object key, Text value, Context context) throws
				IOException, InterruptedException {
					String date = getTime(value.toString());
					if (isDateValid(date)) {
						word.set(date);
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
		job.setJarByClass(Week4_task2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

