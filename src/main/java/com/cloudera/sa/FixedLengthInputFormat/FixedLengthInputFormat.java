package com.cloudera.sa.FixedLengthInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FixedLengthInputFormat extends FileInputFormat<LongWritable, Text>{

	private static String RECORD_BYTE_LENGTH = "fixed.length.record.byte.length";
	
	static public void setRecordLength (Job job, int recordByteLength) {
		job.getConfiguration().set(RECORD_BYTE_LENGTH, Integer.toString(recordByteLength));
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new FixedLengthRecordReader(Integer.parseInt(context.getConfiguration().get(RECORD_BYTE_LENGTH)));
	}




}
