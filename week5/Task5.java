// using BloomFilter to calculate the average post length
// of the student who's reputation is greater than 10
// need three argument, first is forum_stduent file to genarate
// the filter file, second is the forum_node file, third
// is the ouput file
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task5 {

	private static float falsePosRate = 0.0001f;
	private static int numMembers = 5000;

	// three argument, forum_student, forum_node, ouput
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Need three argument: forum_student, forum_node, ouput");
			return;
		}
		// Parse command line arguments
		Path inputFile = new Path(args[0]);
		Path bfFile = new Path("filter");
		// Calculate our vector size and optimal K value based on
		// approximations
		int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);
		int nbHash = getOptimalK(numMembers, vectorSize);
		// Create new Bloom filter
		BloomFilter filter = new BloomFilter(vectorSize, nbHash,
				Hash.MURMUR_HASH);
		System.out.println("Training Bloom filter of size " + vectorSize
				+ " with " + nbHash + " hash functions, " + numMembers
				+ " approximate number of records, and " + falsePosRate
				+ " false positive rate");
		// Open file for read
		String line = null;
		int numElements = 0;
		FileSystem fs = FileSystem.get(new Configuration());
		for (FileStatus status : fs.listStatus(inputFile)) {
			BufferedReader rdr = new BufferedReader(new InputStreamReader(
					fs.open(status.getPath())));
			System.out.println("Reading " + status.getPath());
			String[] fields = null;
			String user_id = null, user_rep = null;
			while ((line = rdr.readLine()) != null) {
				fields = line.split("\t");
				if (fields.length == 5) {
					user_id = fields[0].replace("\"", "").trim();
					user_rep = fields[1].replace("\"", "").trim();
					try {
						if (Integer.parseInt(user_rep) < 11) {
							filter.add(new Key(user_id.getBytes()));
							++numElements;
						}
					} catch (Exception e) {
						continue;
					}
				}
			}
			rdr.close();
		}
		System.out.println("Trained Bloom filter with " + numElements
				+ " entries.");
		System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();
		strm.close();

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "BloomFilter");
		job.setJarByClass(Task5.class);

		job.setMapperClass(BloomFilteringMapper.class);
		job.setReducerClass(BloomFilteringReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		DistributedCache.addCacheFile(new Path("filter").toUri(),
				job.getConfiguration());
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static int getOptimalBloomFilterSize(int numRecords,
			float falsePosRate) {
		int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math
				.pow(Math.log(2), 2));
		return size;
	}

	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize / numMembers * Math.log(2));
	}

	public static class BloomFilteringMapper extends
		Mapper<Object, Text, Text, DoubleWritable> {

		private BloomFilter filter = new BloomFilter();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			// Get file from the DistributedCache
			Path[] paths = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			// Open local file for read.
			for (Path path : paths) {
				if (path.toString().contains("filter")) {
					DataInputStream strm = new DataInputStream(new FileInputStream(
						path.toString()));
					//Read into our Bloom filter.
					filter.readFields(strm);
					strm.close();
				}
			}
		}

		private String dequote(String str) {
			int len = str.length();
			if (len < 3) return "";
			if (str.charAt(0) == '"') {
				return str.substring(1, len-1);
			}
			return str;
		}

		private Boolean isDigit(String str) {
			if (str == null) return false;
			int len = str.length();
			for (int i = 0; i < len; ++i) {
				if (str.charAt(i) > '9' || str.charAt(i) < '0')
					return false;
			}
			return true;
		}

		private int item_count = 0;
		private StringBuilder cur_line = null;
		private String[] items = null;
		private Text key_id = new Text();
		private DoubleWritable  value_len = new DoubleWritable();

		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			String line = value.toString();
			if (item_count == 0) {
				item_count = line.split("\t").length;
				return;
			}
			items = line.split("\t");
			if (items.length > 4 && isDigit(dequote(items[0])) 
					&& isDigit(dequote(items[3]))) {
				if (cur_line != null) {
					items = cur_line.toString().replace("\n", " ").split("\t");
					if (items.length != item_count)
						return;
					String id = dequote(items[3]);
					if (filter.membershipTest(new Key(id.getBytes())))
						return;
					int body_len = dequote(items[4]).length();
					key_id.set(id);
					value_len.set(body_len);
					context.write(key_id, value_len);
				}
				cur_line = new StringBuilder(line);
			} else {
				if (cur_line == null)
					cur_line = new StringBuilder(line);
				cur_line.append(line);
			}
		}
	}

	public static class BloomFilteringReducer
		extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException {
				double sum = 0;
				int cnt = 0;
				for (DoubleWritable val : values) {
					sum = sum + val.get();
					cnt += 1;
				}
				result.set((float)sum / cnt);
				context.write(key, result);
		}
	}
}
