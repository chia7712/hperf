package com.chia7712.hpref.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.util.Bytes;

public class RandomDataFactory {
  private static final List<Character> ASCII_CODE;
  static {
    List<Character> code = new ArrayList<>();
    for (int i = (int)'a'; i <= (int)'z'; ++i) {
      code.add((char)i);
    }
    ASCII_CODE = Collections.unmodifiableList(code);
  }
  private RandomDataFactory() {
  }

  public static RandomData create() {
    return new SimpleRandomData();
  }

  private static class SimpleRandomData implements RandomData {

    private final Random rn = new Random();

    @Override
    public long getLong() {
      return rn.nextLong();
    }

    @Override
    public long getCurrentTimeMs() {
      return System.currentTimeMillis();
    }

    @Override
    public int getInteger() {
      return rn.nextInt();
    }

    @Override
    public short getShort() {
      return (short) rn.nextInt();
    }

    @Override
    public double getDouble() {
      return rn.nextDouble();
    }

    @Override
    public float getFloat() {
      return rn.nextFloat();
    }

    @Override
    public String getStringWithRandomSize(int limit) {
      return getString(Math.max(1, Math.abs(rn.nextInt(limit))));
    }

    @Override
    public String getString(int size) {
      assert size >= 0;
      char[] buf = new char[size];
      for (int i = 0; i != size; ++i) {
        buf[i] = ASCII_CODE.get(rn.nextInt(ASCII_CODE.size()));
      }
      return String.copyValueOf(buf);
    }

    @Override
    public byte[] getBytes(int length) {
      byte[] l = Bytes.toBytes(String.valueOf(length));
      byte[] b = new byte[length];
      rn.nextBytes(b);
      Bytes.putBytes(b, 0, l, 0, l.length);
      return b;
    }

    @Override
    public boolean getBoolean() {
      return rn.nextBoolean();
    }

    @Override
    public int getInteger(int bound) {
      return rn.nextInt(bound);
    }
  }
}