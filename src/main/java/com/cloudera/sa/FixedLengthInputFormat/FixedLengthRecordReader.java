package com.cloudera.sa.FixedLengthInputFormat;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FixedLengthRecordReader  extends RecordReader<LongWritable, Text> {
	
	private final int recordByteLength;
	private long start;
	private long pos;
	private long end;
	private byte[] currentRecord;
	

	private LongWritable key = null;
	private Text value = null;
	
	BufferedInputStream fileIn;
	
	FixedLengthRecordReader ( int recordByteLength) throws IOException {
		this.recordByteLength = recordByteLength;
		System.out.println("FixedLengthRecordReader:" + recordByteLength);
	}
	
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		currentRecord = new byte[recordByteLength];
		FileSplit split = (FileSplit) genericSplit;
		
		start = split.getStart();
	    end = start + split.getLength();
	    final Path file = split.getPath();
	    
	    FileSystem fs = file.getFileSystem(context.getConfiguration());
	    fileIn = new BufferedInputStream(fs.open(split.getPath()));
	    
	    if (start != 0) {
	    	pos = start - (start % recordByteLength) + recordByteLength;
	    	
	        fileIn.skip(pos);
	    }
	    
	    System.out.println("FixedLengthRecordReader:" + start + " " + pos + " " + end);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if (pos >= end) {
			System.out.println("FixedLengthRecordReader Finished end Of Split:" + start + " " + pos + " " + end);
			return false;
		}
		
		if (key == null) {
	      key = new LongWritable();
	    }
	    if (value == null) {
	      value = new Text();
	    }
		
		int result = fileIn.read(currentRecord);
		
		pos += result;
		
		if (result < recordByteLength) {
			System.out.println("FixedLengthRecordReader Finished result is less then length:" + start + " " + pos + " " + end + " (" + result + ")");
			return false;
		}
		
		key.set(pos);
		value.set(currentRecord);
		
		return true;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
	      return 0.0f;
	    } else {
	      return Math.min(1.0f, (pos - start) / (float)(end - start));
	    }
	}

	@Override
	public void close() throws IOException {
		fileIn.close();
	}
 


}
