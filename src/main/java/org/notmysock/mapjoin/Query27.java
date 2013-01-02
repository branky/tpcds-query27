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
import java.nio.*;
import java.util.*;

import org.notmysock.mapjoin.Types.*;
import org.notmysock.mapjoin.Tables.*;


public class Query27 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.job.committer.task.cleanup.needed", false);
        int res = ToolRunner.run(conf, new Query27(), args);
        System.exit(res);
    }

    private Path in;
    private Path out;

    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        if (remainingArgs.length < 2) {
            System.err.println("Usage: Query27 <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }
        String uuid = UUID.randomUUID().toString(); 
        in = new Path(remainingArgs[0]);
        out = new Path(remainingArgs[1]);

        long t1 = System.currentTimeMillis();
        Local27 local = new Local27(getConf());

        Path[] hashes = local.genHashes(in, new Path("file:///tmp/", uuid));

        String tmpfiles = local.resolveFiles(hashes);

        Configuration conf = getConf();

        conf.set("tmpfiles", tmpfiles);
        Job job = new Job(conf, "Query27");
        job.setJarByClass(getClass());

        job.setMapperClass(Mapjoin.class);
        job.setCombinerClass(LimitCombiner.class);
        job.setReducerClass(ReduceAverager.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Stage_1_k.class);
        job.setMapOutputValueClass(Stage_1_v.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(store_sales.InputFormat.class);

        FileInputFormat.addInputPath(job, new Path(in,"store_sales"));
        FileOutputFormat.setOutputPath(job, out);

        //long t1 = System.currentTimeMillis();        
        boolean success = job.waitForCompletion(true);
        long t2 = System.currentTimeMillis();
        System.out.println("Run took " + (t2 - t1) + " milliseconds");
        //System.out.println("We did "+hashes+" getHashCodes over-all");        
        return success ? 0 : 1;
    }

    static final class Mapjoin extends Mapper<LongWritable,store_sales, Stage_1_k, Stage_1_v> {
      Stage_1_k k = new Stage_1_k();
      Stage_1_v v = new Stage_1_v();
      HashMap<Integer, Integer> date_dim_map;
      HashMap<Integer, String> store_map;
      HashMap<Integer, String> item_map;
      HashMap<Integer, Boolean> customer_demographics_map;
      protected void setup(Context context) throws IOException {
        try {
          date_dim_map = (HashMap<Integer,Integer>)readFile("date_dim.hash");
          store_map = (HashMap<Integer, String>)readFile("store.hash");
          item_map = (HashMap<Integer, String>)readFile("item.hash");
          customer_demographics_map = (HashMap<Integer, Boolean>)readFile("customer_demographics.hash");
        } catch(ClassNotFoundException ce) {
          // Integer and String, no worries here
        }
      }
      private Object readFile(String hashFile) throws IOException, ClassNotFoundException {
        ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(new File(hashFile)), 4096));
        return in.readObject();
      }
      protected void map(LongWritable offset, store_sales value, Mapper.Context context) 
        throws IOException, InterruptedException {
        if(date_dim_map.containsKey(Integer.valueOf(value.ss_sold_date_sk)) 
          && customer_demographics_map.containsKey(Integer.valueOf(value.ss_cdemo_sk))) {
          String s_state = store_map.get(Integer.valueOf(value.ss_store_sk));
          String i_item_id = item_map.get(Integer.valueOf(value.ss_item_sk));
          if(s_state != null && i_item_id != null) {
            k.s_state = s_state;
            k.i_item_id = i_item_id;
            v.ss_quantity = value.ss_quantity;
            v.ss_list_price = value.ss_list_price;
            v.ss_coupon_amt = value.ss_coupon_amt;
            v.ss_sales_price = value.ss_sales_price;
            context.write(k, v);
          }
        }
      }
    }

    static final class LimitCombiner extends Reducer<Stage_1_k, Stage_1_v, Stage_1_k, Stage_1_v> {
      int keys = 0;
      @Override
      protected void setup(Context context) throws IOException {
        keys = 0;
      }
      @Override
      public void reduce(Stage_1_k key, Iterable<Stage_1_v> values, Context context) 
        throws IOException, InterruptedException {
        if(keys > 100) return;
        keys++;
        for(Stage_1_v v: values) {
          context.write(key, v);
        }
      }
    }

    static final class ReduceAverager extends Reducer<Stage_1_k, Stage_1_v, Text, Text> {    

      @Override
      public void run(Context context) 
        throws IOException, InterruptedException {
        // duplicated from the reducer
        setup(context);
        for(int i = 0; i < 100 && context.nextKey(); i++) {
          reduce(context.getCurrentKey(), context.getValues(), context);
          // If a back up store is used, reset it
          Iterator<Stage_1_v> iter = context.getValues().iterator();          
          if(iter instanceof ReduceContext.ValueIterator) {
            ((ReduceContext.ValueIterator<Stage_1_v>)iter).resetBackupStore();        
          }
        }
        cleanup(context);
      }

      @Override
      public void reduce(Stage_1_k key, Iterable<Stage_1_v> values, Context context) 
        throws IOException, InterruptedException {
        double sum1 = 0;
        double sum2 = 0;
        double sum3 = 0;
        double sum4 = 0;
        long count = 0;

        for(Stage_1_v v: values) {
          sum1 += v.ss_quantity;
          sum2 += v.ss_list_price;
          sum3 += v.ss_coupon_amt;
          sum4 += v.ss_sales_price;
          count += 1;
        }

        String outKey = String.format("%s\t%s", key.i_item_id, key.s_state);
        String outVal = String.format("%f\t%f\t%f\t%f", sum1/count, sum2/count, sum3/count, sum4/count);
        context.write(new Text(outKey), new Text(outVal));
      }
    }
}
