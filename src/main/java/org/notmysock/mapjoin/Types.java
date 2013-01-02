package org.notmysock.mapjoin;

import java.io.*;
import org.apache.hadoop.io.*;

public class Types
{
  public final static class Stage_1_k implements Writable {
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
  }

  public final static class Stage_1_v implements Writable {
    float ss_quantity;
    float ss_list_price;
    float ss_coupon_amt;
    float ss_sales_price;

    public void write(DataOutput out) throws IOException {
      out.writeFloat(ss_quantity);
      out.writeFloat(ss_list_price);
      out.writeFloat(ss_coupon_amt);
      out.writeFloat(ss_sales_price);  
    }

    public void readFields(DataInput in) throws IOException {
      ss_quantity = in.readFloat();
      ss_list_price = in.readFloat();
      ss_coupon_amt = in.readFloat();
      ss_sales_price = in.readFloat();  
    }
  }
}
