
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


public class Cache27 {

  private FileSplit[] getSplits(String dir) throws IOException {
    File top = new File(dir);
    FileWriter fw = new FileWriter("/tmp/x.log");    
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>();
    for(File part: top.listFiles()) {
      if(part.toString().endsWith(".crc")) continue;
      Path p = new Path(part.toURI());
      FileSplit split = new FileSplit(p, 0, part.length(), null);
      splits.add(split);
    }
    return splits.toArray(new FileSplit[1]);
  }

  public HashMap<Integer,Integer> genDateDim(TaskAttemptContext context) throws IOException { // {
    HashMap<Integer,Integer> data = new HashMap<Integer,Integer>();
    FileSplit[] splits = getSplits("date_dim");
    for(FileSplit split: splits) {
      date_dim.Reader r = new date_dim.Reader();
      r.initialize(split, context);
      while(r.nextKeyValue()) {
        date_dim value = r.getCurrentValue();
        if(value.d_year == 2002) {
          data.put(Integer.valueOf(value.d_date_sk), Integer.valueOf(value.d_year));
        }
      }
    }
    return data;
  }
  
  public HashMap<Integer,String> genStore(TaskAttemptContext context)  throws IOException {
    String[] states = {"SD", "TN"};
    HashMap<Integer,String> data = new HashMap<Integer,String>();
    FileSplit[] splits = getSplits("store");
    for(FileSplit split: splits) {
      store.Reader r = new store.Reader();
      r.initialize(split, context);
      while(r.nextKeyValue()) {
        store value = r.getCurrentValue();
        for(int i = 0; i < states.length; i++) {
          if(states[i].equals(value.s_state)) {
            data.put(Integer.valueOf(value.s_store_sk), states[i]);          
            break;
          }
        }
      }
    }
    return data;
  }

  public  HashMap<Integer,String> genItem(TaskAttemptContext context)  throws IOException {
    HashMap<Integer,String> data = new HashMap<Integer,String>();
    FileSplit[] splits = getSplits("item");
    for(FileSplit split: splits) {
      item.Reader r = new item.Reader();
      r.initialize(split, context);
      while(r.nextKeyValue()) {
        item value = r.getCurrentValue();
        data.put(Integer.valueOf(value.i_item_sk), value.i_item_id);
      }
    }
    return data;
  }

  public HashMap<Integer, Boolean> genCustomerDemographics(TaskAttemptContext context) throws IOException {
    HashMap<Integer,Boolean> data = new HashMap<Integer,Boolean>();
    FileSplit[] splits = getSplits("customer_demographics");
    for(FileSplit split: splits) {
      customer_demographics.Reader r = new customer_demographics.Reader();
      r.initialize(split, context);
      while(r.nextKeyValue()) {
        customer_demographics value = r.getCurrentValue();
        if("M".equals(value.cd_gender) 
            && "S".equals(value.cd_marital_status) 
            && "College".equals(value.cd_education_status)) {
          data.put(Integer.valueOf(value.cd_demo_sk), true);      
        }
      }
    }
    return data;
  }
}
