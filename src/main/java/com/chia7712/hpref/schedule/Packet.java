package com.chia7712.hpref.schedule;

import java.util.Iterator;

public interface Packet extends Iterator<Long> {

  long size();

  void commit();
}
