package HIVE_UDF;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.InputJobInfo;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hive.hcatalog.common.*;
import org.apache.hive.hcatalog.mapreduce.*;
import org.apache.hive.hcatalog.data.*;
import org.apache.hive.hcatalog.data.schema.*;

public class hcatalog_try extends Configured implements Tool {

    public static class Map extends Mapper<WritableComparable, HCatRecord, Text, IntWritable> {
        String groupname;

        @Override
      protected void map( WritableComparable key,
                          HCatRecord value,
                          org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord,
                          Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
            // The group table from /etc/group has name, 'x', id
            groupname = (String) value.get(0);
            int id = (Integer) value.get(2);
            // Just select and emit the name and ID
            context.write(new Text(groupname), new IntWritable(id));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable,
                                       WritableComparable, HCatRecord> {

        protected void reduce( Text key,
                               java.lang.Iterable<IntWritable> values,
                               org.apache.hadoop.mapreduce.Reducer<Text, IntWritable,
                               WritableComparable, HCatRecord>.Context context)
            throws IOException, InterruptedException {
            // Only expecting one ID per group name
            Iterator<IntWritable> iter = values.iterator();
            IntWritable iw = iter.next();
            int id = iw.get();
            // Emit the group name and ID as a record
            HCatRecord record = new DefaultHCatRecord(2);
            record.set(0, key.toString());
            record.set(1, id);
            context.write(null, record);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Get the input and output table names as arguments
        String inputTableName = args[0];
        String outputTableName = args[1];
        // Assume the default database
        String dbName = null;

        Job job = new Job(conf, "UseHCat");
        HCatInputFormat.setInput(job, dbName, inputTableName);
        job.setJarByClass(hcatalog_try.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // An HCatalog record as input
        job.setInputFormatClass(HCatInputFormat.class);

        // Mapper emits a string as key and an integer as value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Ignore the key for the reducer output; emitting an HCatalog record as value
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);
        job.setOutputFormatClass(HCatOutputFormat.class);

        HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName,
                   outputTableName, null));
        HCatSchema s = HCatOutputFormat.getTableSchema(conf);
        System.err.println("INFO: output schema explicitly set for writing:" + s);
        HCatOutputFormat.setSchema(job, s);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new hcatalog_try(), args);
        System.exit(exitCode);
    }
}