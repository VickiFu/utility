/* <copyright>
 <author>Vicki Fu</author>
 <email>vicky.fuyu@gmail.com</email>
 </copyright>
*/
package utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class Join {

  public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// Parser
			String input_key = value.toString().split("\t")[0];
			String input_value = value.toString().split("\t")[1];
			output.collect(new Text(input_key), new Text(input_value));
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringBuilder strBuilder = new StringBuilder();

			while (values.hasNext()) {

				strBuilder.append(values.next().toString());
				strBuilder.append(" ");
			}

			output.collect(key, new Text(strBuilder.toString()));

		}
	}

	public static void main(String[] args) throws IOException {

		if (args.length != 3) {
			System.out.println("This is basic full join, and use tab as the key value delimiter");
			System.out.println("We collect three arguments, input1 input2 output");
		} else {

			JobConf conf = new JobConf(Join.class);
			FileSystem dfs = FileSystem.get(conf);
			dfs.delete(new Path(args[2]));
			conf.setJobName("Join Job");
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(conf, new Path(args[0]));
			FileInputFormat.addInputPath(conf, new Path(args[1]));
			FileOutputFormat.setOutputPath(conf, new Path(args[2]));
			JobClient.runJob(conf);
		}
	}
}
