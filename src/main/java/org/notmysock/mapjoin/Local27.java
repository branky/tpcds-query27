
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


public class Local27 extends Configured {
  private Configuration original;
  public Local27(Configuration conf) {
    super(new Configuration(conf));
    original = new Configuration(conf);
    getConf().set("mapred.framework.name", "local");
  }

  public String resolveFiles(Path[] paths) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for(Path p: paths) {
      String f = p.toUri().getPath();
      if(!first) sb.append(",");
      sb.append(f);
      first = false;
    }
    return sb.toString();
  }

  public Path[] genHashes(Path in, Path out) throws IOException, InterruptedException {
    ArrayList<Path> l = new ArrayList<Path>();
    l.add(genDateDim(in, out));
    l.add(genStore(in, out));
    l.add(genItem(in, out));
    return l.toArray(new Path[1]);
  }

  private Path genDateDim(Path in, Path out) throws IOException, InterruptedException {
    Configuration conf = getConf();
    Job job = new Job(conf, "Query27+date_dim");
    job.setJarByClass(getClass());
    job.setMapperClass(DateDimHash.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(date_dim.InputFormat.class);
    FileInputFormat.addInputPath(job, new Path(in,"date_dim"));
    FileOutputFormat.setOutputPath(job, new Path(out, "date_dim"));
    try {
      boolean success = job.waitForCompletion(true);
    } catch(ClassNotFoundException ce) {
      return null;
    }
    return new Path(new Path(out, "date_dim"), "date_dim.hash");
  }

  static final class DateDimHash extends Mapper<LongWritable,date_dim, Text, Text> {  
    private HashMap<Integer, Integer> data;
    protected void setup(Context context) throws IOException {
      data = new HashMap<Integer,Integer>();
    }
    protected void map(LongWritable offset, date_dim value, Mapper.Context context) 
      throws IOException, InterruptedException {
      if(value.d_year == 2002) {
        data.put(Integer.valueOf(value.d_date_sk), Integer.valueOf(value.d_year));
      }
    }
    protected void cleanup(Context context) throws IOException {
      OutputCommitter out = context.getOutputCommitter();
      if(out instanceof FileOutputCommitter) {
        Path dir = ((FileOutputCommitter)out).getWorkPath();
        FileSystem fs = FileSystem.getLocal(context.getConfiguration());        
        ObjectOutputStream dout = new ObjectOutputStream(fs.create(new Path(dir, "date_dim.hash")));
        dout.writeObject(data);
        dout.close();
      }
    }
  }
  
  private Path genStore(Path in, Path out) throws IOException, InterruptedException {
    Configuration conf = getConf();
    Job job = new Job(conf, "Query27+store");
    job.setJarByClass(getClass());
    job.setMapperClass(StoreHash.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(store.InputFormat.class);    
    FileInputFormat.addInputPath(job, new Path(in,"store"));
    FileOutputFormat.setOutputPath(job, new Path(out, "store"));
    try {
      boolean success = job.waitForCompletion(true);
    } catch(ClassNotFoundException ce) {
      return null;
    }
    return new Path(new Path(out, "store"), "store.hash");
  }

  static final class StoreHash extends Mapper<LongWritable, store, Text, Text> {  
    private HashMap<Integer, String> data;
    private String[] states = {"SD", "TN"};
    protected void setup(Context context) throws IOException {
      data = new HashMap<Integer,String>();
    }
    protected void map(LongWritable offset, store value, Mapper.Context context) 
      throws IOException, InterruptedException {
      for(int i = 0; i < states.length; i++) {
        if(states[i].equals(value.s_state)) {
          data.put(Integer.valueOf(value.s_store_sk), states[i]);          
          break;
        }
      }
    }
    protected void cleanup(Context context) throws IOException {
      OutputCommitter out = context.getOutputCommitter();
      if(out instanceof FileOutputCommitter) {
        Path dir = ((FileOutputCommitter)out).getWorkPath();
        FileSystem fs = FileSystem.getLocal(context.getConfiguration());        
        ObjectOutputStream dout = new ObjectOutputStream(fs.create(new Path(dir, "store.hash")));
        dout.writeObject(data);
        dout.close();
      }
    }
  }

  private Path genItem(Path in, Path out) throws IOException, InterruptedException {
    Configuration conf = getConf();
    Job job = new Job(conf, "Query27+item");
    job.setJarByClass(getClass());
    job.setMapperClass(ItemHash.class);    
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(item.InputFormat.class);    
    FileInputFormat.addInputPath(job, new Path(in,"item"));
    FileOutputFormat.setOutputPath(job, new Path(out, "item"));
    try {
      boolean success = job.waitForCompletion(true);
    } catch(ClassNotFoundException ce) {
      return null;
    }
    return new Path(new Path(out, "item"), "item.hash");
  }

  static final class ItemHash extends Mapper<LongWritable, item, Text, Text> {  
    private HashMap<Integer, String> data;
    private FileWriter fw ;
    protected void setup(Context context) throws IOException {
      data = new HashMap<Integer,String>();      
    fw = new FileWriter("/tmp/items.log");
    }
    protected void map(LongWritable offset, item value, Mapper.Context context) 
      throws IOException, InterruptedException {
      data.put(Integer.valueOf(value.i_item_sk), value.i_item_id);
      fw.write(value.i_item_sk+" : " + value.i_item_id+ "\n");
      fw.flush();
    }
    protected void cleanup(Context context) throws IOException {
      OutputCommitter out = context.getOutputCommitter();
      if(out instanceof FileOutputCommitter) {
        Path dir = ((FileOutputCommitter)out).getWorkPath();
        FileSystem fs = FileSystem.getLocal(context.getConfiguration());        
        ObjectOutputStream dout = new ObjectOutputStream(fs.create(new Path(dir, "item.hash")));
        dout.writeObject(data);
        dout.close();
      }
    }
  }

}
