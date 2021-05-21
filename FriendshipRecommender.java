package hadoop;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class FriendshipRecommender {

	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			String line = value.toString();
			String[] ids = line.split("\t");
			if (ids.length == 2) {
				String uid = ids[0];
				IntWritable ukey = new IntWritable(Integer.parseInt(uid));
				String[] fid = ids[1].split(",");
				for (int i = 0; i < fid.length; i++) {
					String u1 = fid[i];
					IntWritable u1k = new IntWritable(Integer.parseInt(u1));
					// 1 for degree 1 pair (direct friend)
					Text u1v = new Text("1:" + u1);
					context.write(ukey, u1v);
					// System.out.print(u1v);
					// 2 for potential degree 2 pair
					u1v.set("2:" + u1);
					for (int j = i + 1; j < fid.length; j++) {
						String u2 = fid[j];
						IntWritable u2k = new IntWritable(Integer.parseInt(u2));
						//2 for potential degree 2 pair
						Text u2v = new Text("2:" + u2);
						context.write(u1k, u2v);
						context.write(u2k, u1v);
					}
				}
			}
		}
	}
 
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			String[] value;
			HashMap<String, Integer> hash = new HashMap<String, Integer>();
			for (Text val : values) {
				value = (val.toString()).split(":");
				if (value[0].equals("1")) {
					// mark this pair are direct friend
					hash.put(value[1], -1);
				} else if (value[0].equals("2")) {
					if (hash.containsKey(value[1])) {
						// check this pair are or aren't direct friends
						if (hash.get(value[1]) != -1) {
							// if they are not direct friends, count how many common friends
							hash.put(value[1], hash.get(value[1]) + 1);
						}
					} else {
						hash.put(value[1], 1);
					}
				}
			}
			
			// Convert hash to list.
			ArrayList <Entry <String, Integer>> arr = new ArrayList <Entry <String, Integer>>();
			for (Entry<String, Integer> entry : hash.entrySet()) {
				if (entry.getValue() != -1) {   // remove direct friends.
					arr.add(entry);
				}
			}
			// Sort key-value pairs in the list by values (number of common friends).
			Collections.sort(arr, new Comparator<Entry<String, Integer>>() {
				public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
					return e2.getValue().compareTo(e1.getValue());
				}
			});
			// Output at most 10 potential friends
			ArrayList<String> friend_list = new ArrayList<String>();
			// if there are less than 10 friends, output all of them
			for (int i = 0; i < Math.min(10, arr.size()); i++) {
				friend_list.add(arr.get(i).getKey());
			}
			context.write(key, new Text(StringUtils.join(friend_list, ",")));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "FriendshipRecommender");
		job.setJarByClass(FriendshipRecommender.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}