package org.notmysock.mapjoin;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;


import java.io.*;
import java.util.*;


public class Query3 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.job.committer.task.cleanup.needed", false);
        int res = ToolRunner.run(conf, new Query3(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        if (remainingArgs.length < 2) {
            System.err.println("Usage: Query3 <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }
        /*

        Configuration conf = getConf();

        Job job = new Job(conf, "TestSort");
        job.setJarByClass(getClass());

        job.setMapperClass(WordReader.class);
        job.setReducerClass(WordCounter.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setSortComparatorClass(OrderByComparator.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        long t1 = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long t2 = System.currentTimeMillis();
        System.out.println("Run took " + (t2 - t1) + " milliseconds");
        System.out.println("We did "+compares+" comparisons over-all");
        //System.out.println("We did "+hashes+" getHashCodes over-all");        
        return success ? 0 : 1;
        */
        return 1;
    }
}
