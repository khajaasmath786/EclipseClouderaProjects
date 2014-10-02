import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CheckNullColumnMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		String columnsToBeValidated = "";
		columnsToBeValidated = conf.get("columnsToBeValidated");
		String SEP = ",";

		/*
		 * Get the FileSplit for the input file, which provides access to the
		 * file's path. You need the file's path because it contains the name of
		 * the play.
		 */
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		Path path = fileSplit.getPath();

		String wordPlace = path.getName();

		Text location = new Text(wordPlace);

		/*
		 * Convert the line to lower case.
		 */
		String lc_line = key.toString().toLowerCase();

		String emptyColumns = "";
		String[] columnValues = lc_line.split(SEP);
		// Check if line has any empty data with columns passed as parameter
		if (columnValues.length > 1 && (!lc_line.trim().equals(""))
				&& (!columnsToBeValidated.trim().equals(""))) {
			String validateColumns[] = columnsToBeValidated.split(SEP);
			emptyColumns = getColumnsWithEmptyData(columnValues,
					validateColumns);
			if (!emptyColumns.trim().equals("")) {
				// Generate InputFile with LineData and Empty Columns
				context.write(location, new Text(key.toString() + "\n"
						+ emptyColumns));
			}

		}
		// Check if line has any empty data without any columns passed.
		if (columnValues.length > 1 && (!lc_line.trim().equals(""))
				&& columnsToBeValidated.trim().equals("")) {
			emptyColumns = getColumnsWithEmptyData(columnValues);
			// Generate InputFile with LineData and Empty Columns
			context.write(location, new Text(key.toString() + "\n"
					+ emptyColumns));
		}

	}

	/*
	 * 
	 * Check if there are any empty columns in input file with the column
	 * numbers passed as parameter
	 */
	public String getColumnsWithEmptyData(String[] input, String[] columns) {
		String SEP = ",";
		boolean firstValue = true;
		StringBuilder valueList = new StringBuilder();
		for (String value : columns) {

			String checkColumnData = input[Integer.parseInt(value) - 1];

			/*
			 * If this is not the word's first location, add a comma to the end
			 * of valueList.
			 */
			if (checkColumnData == null || checkColumnData.trim().equals("")) {
				if (!firstValue) {
					valueList.append(SEP);
				} else {
					firstValue = false;
				}

				/*
				 * Convert the location to a String and append it to valueList.
				 */
				valueList.append(value);
			}

		}
		return valueList.toString();

	}

	/*
	 * 
	 * Check if there are any empty columns in entire input files
	 */
	public String getColumnsWithEmptyData(String[] input) {
		String SEP = ",";
		boolean firstValue = true;
		StringBuilder valueList = new StringBuilder();
		for (int i = 0; i < input.length; i++) {

			/*
			 * If this is not the word's first location, add a comma to the end
			 * of valueList.
			 */
			if (input[i] == null || input[i].trim().equals("")) {
				if (!firstValue) {
					valueList.append(SEP);
				} else {
					firstValue = false;
				}

				/*
				 * Convert the location to a String and append it to valueList.
				 */
				valueList.append((i + 1));
			}

		}
		return valueList.toString();

	}
}