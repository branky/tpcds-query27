
package org.notmysock.mapjoin;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

/*
struct date_dim { i32 d_date_sk, string d_date_id, timestamp d_date, i32 d_month_seq, i32 d_week_seq, i32 d_quarter_seq, i32 d_year, i32 d_dow, i32 d_moy, i32 d_dom, i32 d_qoy, i32 d_fy_year, i32 d_fy_quarter_seq, i32 d_fy_week_seq, string d_day_name, string d_quarter_name, string d_holiday, string d_weekend, string d_following_holiday, i32 d_first_dom, i32 d_last_dom, i32 d_same_day_ly, i32 d_same_day_lq, string d_current_day, string d_current_week, string d_current_month, string d_current_quarter, string d_current_year}

struct store { i32 s_store_sk, string s_store_id, timestamp s_rec_start_date, timestamp s_rec_end_date, i32 s_closed_date_sk, string s_store_name, i32 s_number_employees, i32 s_floor_space, string s_hours, string s_manager, i32 s_market_id, string s_geography_class, string s_market_desc, string s_market_manager, i32 s_division_id, string s_division_name, i32 s_company_id, string s_company_name, string s_street_number, string s_street_name, string s_street_type, string s_suite_number, string s_city, string s_county, string s_state, string s_zip, string s_country, float s_gmt_offset, float s_tax_precentage} 

struct customer_demographics { i32 cd_demo_sk, string cd_gender, string cd_marital_status, string cd_education_status, i32 cd_purchase_estimate, string cd_credit_rating, i32 cd_dep_count, i32 cd_dep_employed_count, i32 cd_dep_college_count} 

struct item { i32 i_item_sk, string i_item_id, timestamp i_rec_start_date, timestamp i_rec_end_date, string i_item_desc, float i_current_price, float i_wholesale_cost, i32 i_brand_id, string i_brand, i32 i_class_id, string i_class, i32 i_category_id, string i_category, i32 i_manufact_id, string i_manufact, string i_size, string i_formulation, string i_color, string i_units, string i_container, i32 i_manager_id, string i_product_name}

struct store_sales { i32 ss_sold_date_sk, i32 ss_sold_time_sk, i32 ss_item_sk, i32 ss_customer_sk, i32 ss_cdemo_sk, i32 ss_hdemo_sk, i32 ss_addr_sk, i32 ss_store_sk, i32 ss_promo_sk, i32 ss_ticket_number, i32 ss_quantity, float ss_wholesale_cost, float ss_list_price, float ss_sales_price, float ss_ext_discount_amt, float ss_ext_sales_price, float ss_ext_wholesale_cost, float ss_ext_list_price, float ss_ext_tax, float ss_coupon_amt, float ss_net_paid, float ss_net_paid_inc_tax, float ss_net_profit}
*/

public class Tables extends Utils {
  public static final class date_dim {
    int d_date_sk;
    int d_year;
    public static final class InputFormat 
      extends FileInputFormat<LongWritable, date_dim> {      
      @Override
      public RecordReader<LongWritable,date_dim> createRecordReader(InputSplit input,  TaskAttemptContext context)
          throws IOException, InterruptedException {          
        RecordReader<LongWritable, date_dim> reader =  new date_dim.Reader();
        reader.initialize(input, context);
        return reader;
      }
      @Override
      protected boolean isSplitable(JobContext context, Path filename) {
        return false;
      }
    }

    public static final class Reader extends RecordReader<LongWritable,date_dim> {
      private LineRecordReader lineReader;
      private LongWritable key = new LongWritable();
      private date_dim value = new date_dim();

      public Reader() {
        lineReader = new LineRecordReader();
      }
      
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        lineReader.initialize(split, context);
      }

      public LongWritable getCurrentKey() { return key; }
      public date_dim getCurrentValue() { return value; }

      public boolean nextKeyValue() throws IOException {
        // get the next line
        if (!lineReader.nextKeyValue()) {
          return false;
        }
        LongWritable lineKey = lineReader.getCurrentKey();
        Text lineValue = lineReader.getCurrentValue();

        key.set(lineKey.get());
        String [] pieces = lineValue.toString().split("\\|");
        if(pieces.length > 6) {
          value.d_year = parseInt(pieces[6]);
          value.d_date_sk = parseInt(pieces[0]);
        }
        return true;
      }

      public void close() throws IOException {
        lineReader.close();
      }

      public float getProgress() throws IOException {
        return lineReader.getProgress();
      }
    }
  }

  public static final class store {
    int s_store_sk;
    String s_state;
    public static final class InputFormat 
      extends FileInputFormat<LongWritable, store> {      
      @Override
      public RecordReader<LongWritable,store> createRecordReader(InputSplit input,  TaskAttemptContext context)
          throws IOException, InterruptedException {          
        RecordReader<LongWritable, store> reader =  new store.Reader();
        reader.initialize(input, context);
        return reader;
      }
      @Override
      protected boolean isSplitable(JobContext context, Path filename) {
        return false;
      }
    }

    public static final class Reader extends RecordReader<LongWritable,store> {
      private LineRecordReader lineReader;
      private LongWritable key = new LongWritable();
      private store value = new store();

      public Reader() {
        lineReader = new LineRecordReader();
      }
      
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        lineReader.initialize(split, context);
      }

      public LongWritable getCurrentKey() { return key; }
      public store getCurrentValue() { return value; }

      public boolean nextKeyValue() throws IOException {
        // get the next line
        if (!lineReader.nextKeyValue()) {
          return false;
        }
        LongWritable lineKey = lineReader.getCurrentKey();
        Text lineValue = lineReader.getCurrentValue();

        key.set(lineKey.get());
        String [] pieces = lineValue.toString().split("\\|");
        if(pieces.length > 24) {
          value.s_state = pieces[24];
          value.s_store_sk = parseInt(pieces[0]);
        }
        return true;
      }

      public void close() throws IOException {
        lineReader.close();
      }

      public float getProgress() throws IOException {
        return lineReader.getProgress();
      }
    }
  }

  public static final class customer_demographics {
    int cd_demo_sk;
    String cd_gender;
    String cd_marital_status;
    String cd_education_status;

    public static final class InputFormat 
      extends FileInputFormat<LongWritable, customer_demographics> {      
      @Override
      public RecordReader<LongWritable,customer_demographics> createRecordReader(InputSplit input,  TaskAttemptContext context)
          throws IOException, InterruptedException {          
        RecordReader<LongWritable, customer_demographics> reader =  new customer_demographics.Reader();
        reader.initialize(input, context);
        return reader;
      }
      @Override
      protected boolean isSplitable(JobContext context, Path filename) {
        return false;
      }
    }

    public static final class Reader extends RecordReader<LongWritable,customer_demographics> {
      private LineRecordReader lineReader;
      private LongWritable key = new LongWritable();
      private customer_demographics value = new customer_demographics();

      public Reader() {
        lineReader = new LineRecordReader();
      }
      
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        lineReader.initialize(split, context);
      }

      public LongWritable getCurrentKey() { return key; }
      public customer_demographics getCurrentValue() { return value; }

      public boolean nextKeyValue() throws IOException {
        // get the next line
        if (!lineReader.nextKeyValue()) {
          return false;
        }
        LongWritable lineKey = lineReader.getCurrentKey();
        Text lineValue = lineReader.getCurrentValue();

        key.set(lineKey.get());
        String [] pieces = lineValue.toString().split("\\|");

        switch(pieces.length-1) {
          default: 
          case 3: value.cd_education_status = pieces[3];
          case 2: value.cd_marital_status = pieces[2];
          case 1: value.cd_gender = pieces[1];
          case 0: value.cd_demo_sk = parseInt(pieces[0]);
        }
        return true;
      }

      public void close() throws IOException {
        lineReader.close();
      }

      public float getProgress() throws IOException {
        return lineReader.getProgress();
      }
    }
  }

  public static final class item {
    int i_item_sk;
    String i_item_id;
    public static final class InputFormat 
      extends FileInputFormat<LongWritable, item> {      
      @Override
      public RecordReader<LongWritable,item> createRecordReader(InputSplit input,  TaskAttemptContext context)
          throws IOException, InterruptedException {          
        RecordReader<LongWritable, item> reader =  new item.Reader();
        reader.initialize(input, context);
        return reader;
      }
      @Override
      protected boolean isSplitable(JobContext context, Path filename) {
        return false;
      }
    }

    public static final class Reader extends RecordReader<LongWritable,item> {
      private LineRecordReader lineReader;
      private LongWritable key = new LongWritable();
      private item value = new item();

      public Reader() {
        lineReader = new LineRecordReader();
      }
      
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        lineReader.initialize(split, context);
      }

      public LongWritable getCurrentKey() { return key; }
      public item getCurrentValue() { return value; }

      public boolean nextKeyValue() throws IOException {
        // get the next line
        if (!lineReader.nextKeyValue()) {
          return false;
        }
        LongWritable lineKey = lineReader.getCurrentKey();
        Text lineValue = lineReader.getCurrentValue();

        key.set(lineKey.get());
        String [] pieces = lineValue.toString().split("\\|");
        if(pieces.length > 2) {
          value.i_item_id = pieces[1];
          value.i_item_sk = parseInt(pieces[0]);
        }
        return true;
      }

      public void close() throws IOException {
        lineReader.close();
      }

      public float getProgress() throws IOException {
        return lineReader.getProgress();
      }
    }
  }

  public static class store_sales implements Writable {
    int ss_sold_date_sk;
    int ss_sold_time_sk;
    int ss_item_sk;
    int ss_customer_sk;
    int ss_cdemo_sk;
    int ss_hdemo_sk;
    int ss_addr_sk;
    int ss_store_sk;
    int ss_promo_sk;
    int ss_ticket_number;
    int ss_quantity;
    float ss_wholesale_cost;
    float ss_list_price;
    float ss_sales_price;
    float ss_ext_discount_amt;
    float ss_ext_sales_price;
    float ss_ext_wholesale_cost;
    float ss_ext_list_price;
    float ss_ext_tax;
    float ss_coupon_amt;
    float ss_net_paid;
    float ss_net_paid_inc_tax;
    float ss_net_profit;
    
    public void write(DataOutput out) throws IOException {
      out.writeInt(ss_sold_date_sk);
      out.writeInt(ss_sold_time_sk);
      out.writeInt(ss_item_sk);
      out.writeInt(ss_customer_sk);
      out.writeInt(ss_cdemo_sk);
      out.writeInt(ss_hdemo_sk);
      out.writeInt(ss_addr_sk);
      out.writeInt(ss_store_sk);
      out.writeInt(ss_promo_sk);
      out.writeInt(ss_ticket_number);
      out.writeInt(ss_quantity);
      out.writeFloat(ss_wholesale_cost);
      out.writeFloat(ss_list_price);
      out.writeFloat(ss_sales_price);
      out.writeFloat(ss_ext_discount_amt);
      out.writeFloat(ss_ext_sales_price);
      out.writeFloat(ss_ext_wholesale_cost);
      out.writeFloat(ss_ext_list_price);
      out.writeFloat(ss_ext_tax);
      out.writeFloat(ss_coupon_amt);
      out.writeFloat(ss_net_paid);
      out.writeFloat(ss_net_paid_inc_tax);
      out.writeFloat(ss_net_profit);
    }

    public void readFields(DataInput in) throws IOException {
      this.ss_sold_date_sk = in.readInt();
      this.ss_sold_time_sk = in.readInt();
      this.ss_item_sk = in.readInt();
      this.ss_customer_sk = in.readInt();
      this.ss_cdemo_sk = in.readInt();
      this.ss_hdemo_sk = in.readInt();
      this.ss_addr_sk = in.readInt();
      this.ss_store_sk = in.readInt();
      this.ss_promo_sk = in.readInt();
      this.ss_ticket_number = in.readInt();
      this.ss_quantity = in.readInt();
      this.ss_wholesale_cost = in.readFloat();
      this.ss_list_price = in.readFloat();
      this.ss_sales_price = in.readFloat();
      this.ss_ext_discount_amt = in.readFloat();
      this.ss_ext_sales_price = in.readFloat();
      this.ss_ext_wholesale_cost = in.readFloat();
      this.ss_ext_list_price = in.readFloat();
      this.ss_ext_tax = in.readFloat();
      this.ss_coupon_amt = in.readFloat();
      this.ss_net_paid = in.readFloat();
      this.ss_net_paid_inc_tax = in.readFloat();
      this.ss_net_profit = in.readFloat();
    }


    public static final class InputFormat 
      extends FileInputFormat<LongWritable, store_sales> {
      @Override
      public RecordReader<LongWritable,store_sales> createRecordReader(InputSplit input,  TaskAttemptContext context)
          throws IOException, InterruptedException {          
        RecordReader<LongWritable, store_sales> reader =  new store_sales.Reader();
        reader.initialize(input, context);
        return reader;
      }
    }

    public static final class Reader extends RecordReader<LongWritable,store_sales> {
      private LineRecordReader lineReader;
      private LongWritable key = new LongWritable();
      private store_sales value = new store_sales();

      public Reader() {
        lineReader = new LineRecordReader();
      }
      
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        lineReader.initialize(split, context);
      }

      public LongWritable getCurrentKey() { return key; }
      public store_sales getCurrentValue() { return value; }

      public boolean nextKeyValue() throws IOException {
        // get the next line
        if (!lineReader.nextKeyValue()) {
          return false;
        }
        LongWritable lineKey = lineReader.getCurrentKey();
        Text lineValue = lineReader.getCurrentValue();

        key.set(lineKey.get());
        String [] pieces = lineValue.toString().split("\\|");

        switch(pieces.length-1) {        
          case 22: value.ss_net_profit = parseFloat(pieces[22]);
          case 21: value.ss_net_paid_inc_tax = parseFloat(pieces[21]);
          case 20: value.ss_net_paid = parseFloat(pieces[20]);
          case 19: value.ss_coupon_amt = parseFloat(pieces[19]);
          case 18: value.ss_ext_tax = parseFloat(pieces[18]);
          case 17: value.ss_ext_list_price = parseFloat(pieces[17]);
          case 16: value.ss_ext_wholesale_cost = parseFloat(pieces[16]);
          case 15: value.ss_ext_sales_price = parseFloat(pieces[15]);
          case 14: value.ss_ext_discount_amt = parseFloat(pieces[14]);
          case 13: value.ss_sales_price = parseFloat(pieces[13]);
          case 12: value.ss_list_price = parseFloat(pieces[12]);
          case 11: value.ss_wholesale_cost = parseFloat(pieces[11]);
          case 10: value.ss_quantity = parseInt(pieces[10]);
          case 9: value.ss_ticket_number = parseInt(pieces[9]);
          case 8: value.ss_promo_sk = parseInt(pieces[8]);
          case 7: value.ss_store_sk = parseInt(pieces[7]);
          case 6: value.ss_addr_sk = parseInt(pieces[6]);
          case 5: value.ss_hdemo_sk = parseInt(pieces[5]);
          case 4: value.ss_cdemo_sk = parseInt(pieces[4]);
          case 3: value.ss_customer_sk = parseInt(pieces[3]);
          case 2: value.ss_item_sk = parseInt(pieces[2]);
          case 1: value.ss_sold_time_sk = parseInt(pieces[1]);
          case 0: value.ss_sold_date_sk = parseInt(pieces[0]);
        }
        return true;
      }

      public void close() throws IOException {
        lineReader.close();
      }

      public float getProgress() throws IOException {
        return lineReader.getProgress();
      }
    }
  }

  // there is no reason for inheritance, I'm merely saving typing
  public static final class store_sales_seq extends store_sales {
    public static final class InputFormat 
      extends SequenceFileInputFormat<LongWritable, store_sales_seq> {
      @Override
      public RecordReader<LongWritable,store_sales_seq> createRecordReader(InputSplit input,  TaskAttemptContext context)
          throws IOException {          
        try {
          RecordReader<LongWritable, store_sales_seq> reader =  new store_sales_seq.Reader();
          reader.initialize(input, context);
          return reader;
        } catch(InterruptedException ie) {
        }
        return null;
      }
    }

    public static final class Reader extends RecordReader<LongWritable,store_sales_seq> {
      private SequenceFileRecordReader seqReader;
      private LongWritable key = new LongWritable();
      private store_sales_seq value = new store_sales_seq();

      public Reader() {
        seqReader = new SequenceFileRecordReader<LongWritable,store_sales_seq>();
      }
      
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        seqReader.initialize(split, context);
      }

      public LongWritable getCurrentKey() { return key; }
      public store_sales_seq getCurrentValue() { return value; }

      public boolean nextKeyValue() throws IOException, InterruptedException {
        // get the next line
        if (!seqReader.nextKeyValue()) {
          return false;
        }
        LongWritable lineKey = (LongWritable)seqReader.getCurrentKey();
        key.set(lineKey.get());
        value = (store_sales_seq)seqReader.getCurrentValue();
        return true;
      }

      public void close() throws IOException {
        seqReader.close();
      }

      public float getProgress() throws IOException {
        return seqReader.getProgress();
      }
    }
  }
}
