

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

public class ConverTable extends Configured {
  private Configuration original;
  private ArrayList<Job> jobs = new ArrayList<Job>();
  public ConverTable(Configuration conf) {
    super(new Configuration(conf));
    original = new Configuration(conf);
  }

  public Path genStoreSalesRc(Path in) 
    throws IOException, InterruptedException {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path rcpath = new Path(in, "store_sales_seq");
    if(fs.exists(rcpath)) return rcpath;

    Job job = new Job(conf, "Query27+store_sales_seq");
    job.setJarByClass(getClass());
    job.setMapperClass(StoreSalesConvert.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(store_sales_seq.class);
    job.setInputFormatClass(store_sales.InputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(in,"store_sales"));
    FileOutputFormat.setOutputPath(job, rcpath);
    try {
      job.waitForCompletion(true);
    } catch(ClassNotFoundException ce) {
      return null;
    }
    return rcpath;
  }
  static final class StoreSalesConvert extends Mapper<LongWritable,store_sales, LongWritable, store_sales_seq> {  
    store_sales_seq v = new store_sales_seq();
    protected void map(LongWritable offset, store_sales value, Mapper.Context context) 
      throws IOException, InterruptedException {
      v.ss_sold_date_sk = value.ss_sold_date_sk;
      v.ss_sold_time_sk = value.ss_sold_time_sk;
      v.ss_item_sk = value.ss_item_sk;
      v.ss_customer_sk = value.ss_customer_sk;
      v.ss_cdemo_sk = value.ss_cdemo_sk;
      v.ss_hdemo_sk = value.ss_hdemo_sk;
      v.ss_addr_sk = value.ss_addr_sk;
      v.ss_store_sk = value.ss_store_sk;
      v.ss_promo_sk = value.ss_promo_sk;
      v.ss_ticket_number = value.ss_ticket_number;
      v.ss_quantity = value.ss_quantity;
      v.ss_wholesale_cost = value.ss_wholesale_cost;
      v.ss_list_price = value.ss_list_price;
      v.ss_sales_price = value.ss_sales_price;
      v.ss_ext_discount_amt = value.ss_ext_discount_amt;
      v.ss_ext_sales_price = value.ss_ext_sales_price;
      v.ss_ext_wholesale_cost = value.ss_ext_wholesale_cost;
      v.ss_ext_list_price = value.ss_ext_list_price;
      v.ss_ext_tax = value.ss_ext_tax;
      v.ss_coupon_amt = value.ss_coupon_amt;
      v.ss_net_paid = value.ss_net_paid;
      v.ss_net_paid_inc_tax = value.ss_net_paid_inc_tax;
      v.ss_net_profit = value.ss_net_profit;
      context.write(offset, v);
    }
  }
}
