import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CheckNullColumnReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String lineData = " ";
		String emptyColumns = " ";

		for (Text value : values) {
			String[] valTokens = value.toString().split("\n");
			lineData = valTokens[0];
			emptyColumns = valTokens[1];
			// Generate InputFile with LineData and Empty Columns
			context.write(new Text(key), new Text(lineData + " EmptyColumn "
					+ emptyColumns));
		}
	}
}