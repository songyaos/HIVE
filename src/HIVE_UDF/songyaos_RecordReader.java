package HIVE_UDF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapred.RecordReader;




public class songyaos_RecordReader implements RecordReader<LongWritable, Text> {

	private static final Log LOG = LogFactory.getLog(songyaos_RecordReader.class.getName());
	//private static final int NUMBER_OF_FIELDS = 4;//227;
	
	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	@SuppressWarnings("deprecation")
	private LineReader lineReader;
	int maxLineLength;
	
	public songyaos_RecordReader(FileSplit inputSplit, Configuration job) throws IOException {
		maxLineLength = job.getInt("mapred.songyaos_linereader.maxlength", Integer.MAX_VALUE);
		start = inputSplit.getStart();
		end = start + inputSplit.getLength();
		final Path file = inputSplit.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);
		
		// Open file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(file);
		boolean skipFirstLine = false;
		if (codec != null) {
			lineReader = new LineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			lineReader = new LineReader(fileIn, job);
		}
		
		if (skipFirstLine) {
			start += lineReader.readLine(new Text(), 0, (int)Math.min((long)Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}
	
	
	public LongWritable createKey() {
		return new LongWritable();
	}
	
	public Text createValue() {
		return new Text();
	}
	
	/**
	 * Reads the next record in the split.  All instances of \\t, \\n and \\r\n are replaced by a space.
	 * @param key key of the record which will map to the byte offset of the record's line
	 * @param value the record in text format
	 * @return true if a record existed, false otherwise
	 * @throws IOException
	 */
	public synchronized boolean next(LongWritable key, Text value) throws IOException {
		// Stay within the split
		Text temp = new Text();
		String str ="";
		while (pos < end) {
			key.set(pos);
			int newSize = lineReader.readLine(temp, maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
			
			if (newSize == 0)
				return false;
			
			if (temp.toString().equals("--")){
				value.set(str);
				return true;
			}
			else{
				str += temp.toString().split("=")[1] + "\t";
			}
//			String str = value.toString().replaceAll("\\\\\t", " ").replaceAll("\\\\(\n|\r|\r\n)", "");
//			String[] fields = str.split("\t", -1);
			
			
			pos += newSize;
			
//			if (newSize < maxLineLength && fields.length == NUMBER_OF_FIELDS)
//				return true;
//			
//			if (fields.length != NUMBER_OF_FIELDS)
//				LOG.warn("Skipped line at position " + (pos - newSize) + " with incorrect number of fields (expected "+ NUMBER_OF_FIELDS + " but found " + fields.length + ")");
//			else
//				LOG.warn("Skipped line of size " + newSize + " at position " + (pos - newSize));
		}
		return false;
	}
	
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float)(end - start));
		}
	}
	
	public synchronized long getPos() throws IOException {
		return pos;
	}
	
	public synchronized void close() throws IOException {
		if (lineReader != null)
			lineReader.close();
	}
}
