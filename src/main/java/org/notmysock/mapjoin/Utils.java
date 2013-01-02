package org.notmysock.mapjoin;

public class Utils {
  public static int parseInt(String s) {
    if(s.length() == 0) {
      return 0;
    }
    return Integer.parseInt(s);
  }

  public static float parseFloat(String s) {
    if(s.length() == 0) {
      return 0;
    }
    return Float.parseFloat(s);
  }
}
