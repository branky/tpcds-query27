package org.notmysock.mapjoin;

import java.io.*;
import java.nio.*;
import org.apache.hadoop.io.*;

public class Types
{
  public final static class Stage_1_k implements WritableComparable {
    int i_item_id;
    String s_state;

    public void write(DataOutput out) throws IOException {
      out.writeInt(i_item_id);      
      out.writeUTF(s_state);
    }

    public void readFields(DataInput in) throws IOException {
      i_item_id = in.readInt();
      s_state = in.readUTF();
    }

    public int compareTo(Stage_1_k o) {
      if(i_item_id != o.i_item_id) {
        return Integer.valueOf(i_item_id).compareTo(o.i_item_id);
      }
      return s_state.compareTo(o.s_state);
    }

    @Override
    public int compareTo(Object o) {
      return compareTo((Stage_1_k)o);
    }
  }

  public final static class Stage_1_v implements Writable {
    int ss_quantity;
    float ss_list_price;
    float ss_coupon_amt;
    float ss_sales_price;

    public void write(DataOutput out) throws IOException {
      out.writeInt(ss_quantity);
      out.writeFloat(ss_list_price);
      out.writeFloat(ss_coupon_amt);
      out.writeFloat(ss_sales_price);  
    }

    public void readFields(DataInput in) throws IOException {
      ss_quantity = in.readInt();
      ss_list_price = in.readFloat();
      ss_coupon_amt = in.readFloat();
      ss_sales_price = in.readFloat();  
    }
  }
}
