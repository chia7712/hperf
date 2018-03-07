package com.chia7712.hperf.operation;

import java.io.IOException;

public interface Slave extends AutoCloseable, Statisticable {
  void updateRow(RowWork work) throws IOException, InterruptedException;
  ProcessMode getProcessMode();
  RequestMode getRequestMode();

}