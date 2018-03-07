package com.chia7712.hperf.schedule;

import java.util.Iterator;

public interface Packet extends Iterator<Long> {

  long size();

  void commit();
}
