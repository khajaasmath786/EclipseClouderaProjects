/*
 * @Author: KhajaAsmath
 * 
 * */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CheckNullColumn extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length < 1) {
			System.out
					.printf("Usage: CheckNullColumn <input dir> <output dir> <Enter Columns to Check Empty String>\n");
			return -1;
		}
		// Input/Indexing/invertedIndexInput Output/Indexing
		Configuration conf = new Configuration();
		if (args.length > 2) {
			// Columns to be validated
			conf.set("columnsToBeValidated", args[2]);
		} else {
			// Entire file is validated if parameters are not passed
			conf.set("columnsToBeValidated", " ");
		}
		// conf.set("columnsToBeValidated", conf.get("Columns"));

		Job job = new Job(conf);
		job.setJarByClass(CheckNullColumn.class);
		job.setJobName("CheckNullColumn");

		/*
		 * We are using a KeyValueText file as the input file. Therefore, we
		 * must call setInputFormatClass. There is no need to call
		 * setOutputFormatClass, because the application uses a text file for
		 * output.
		 */
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(CheckNullColumnMapper.class);
		job.setReducerClass(CheckNullColumnReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new CheckNullColumn(), args);
		System.exit(exitCode);
	}
}
