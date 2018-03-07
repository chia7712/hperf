package com.chia7712.hperf.data;

public interface RandomData {

  boolean getBoolean();

  long getLong();

  long getCurrentTimeMs();

  int getInteger();

  int getInteger(int bound);

  short getShort();

  double getDouble();

  float getFloat();

  String getStringWithRandomSize(int limit);

  String getString(int size);

  byte[] getBytes(int size);
}
