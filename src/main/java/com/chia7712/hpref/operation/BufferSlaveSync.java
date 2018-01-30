package com.chia7712.hpref.operation;

import java.io.IOException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

public class BufferSlaveSync extends BatchSlave {

  private final BufferedMutator mutater;
  private final boolean closeBuffer;

  public BufferSlaveSync(BufferedMutator mutater, final DataStatistic statistic, final int batchSize
    , boolean closeBuffer) {
    super(statistic, batchSize);
    this.mutater = mutater;
    this.closeBuffer = closeBuffer;
  }

  @Override
  public void updateRow(RowWork work) throws IOException, InterruptedException {
    Row row = prepareRow(work);
    switch (work.getDataType()) {
    case PUT:
      mutater.mutate((Put) row);
      finishRows(work.getDataType(), 1);
      break;
    default:
      throw new IllegalArgumentException("Unsupported type:" + work.getDataType());
    }
  }

  @Override
  public ProcessMode getProcessMode() {
    return closeBuffer ? ProcessMode.BUFFER : ProcessMode.SHARED_BUFFER;
  }

  @Override
  public RequestMode getRequestMode() {
    return RequestMode.NORMAL;
  }

  @Override
  public void close() throws IOException, InterruptedException {
    if (closeBuffer) {
      mutater.close();
    }
  }
}
