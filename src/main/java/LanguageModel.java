
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;
		// get the threashold parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threashold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String[] input = value.toString().trim().split("\t");
			if (input.length != 2) {
				return;
			}

			String[] words = input[0].trim().split("\\s+");
			int count = Integer.parseInt(input[1]);
			if (count < threashold) {
				return;
			}

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; ++i) {
				sb.append(words[i]);
				sb.append(" ");
			}

			String outputKey = sb.toString().trim();
			String outputValue = words[words.length - 1] + "=" + count;

			//this is --> cool = 20
			if (outputKey != null && outputKey.length() > 0) {
				context.write(new Text(outputKey), new Text(outputValue));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//this is, <girl = 50, boy = 60>
			TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
			for (Text value : values) {
				String[] input = value.toString().trim().split("=");
				String word = input[0];
				int count = Integer.parseInt(input[1]);
				if (!tm.containsKey(count)) {
					tm.put(count, new ArrayList<String>());
				}
				tm.get(count).add(word);
			}

			//<50, <girl, bird>> <60, <boy...>>
			Iterator<Integer> iterator = tm.keySet().iterator();
			for (int j = 0; j < n && iterator.hasNext();) {
				int curtCount = iterator.next();
				List<String> curtList = tm.get(curtCount);
				for (String curtWord : curtList) {
					context.write(new DBOutputWritable(key.toString(), curtWord, curtCount), NullWritable.get());
					j++;
				}
			}

		}
	}
}
