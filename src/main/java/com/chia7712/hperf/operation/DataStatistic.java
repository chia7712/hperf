package com.chia7712.hperf.operation;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

public class DataStatistic {

  private final LongAdder processingRows = new LongAdder();
  private final LongAdder committedRows = new LongAdder();
  private final ConcurrentMap<Record, LongAdder> statistic = new ConcurrentSkipListMap<>();

  public void addNewRows(Record record, long delta) {
    assert delta >= 0;
    processingRows.add(delta);
  }

  public void finishRows(Record record, long delta) {
    assert delta >= 0;
    statistic.computeIfAbsent(record, k -> new LongAdder())
      .add(delta);
    processingRows.add(-delta);
    committedRows.add(delta);
  }

  public long getCommittedRows() {
    return committedRows.longValue();
  }

  public long getProcessingRows() {
    return processingRows.longValue();
  }

  public void consume(BiConsumer<Record, LongAdder> f) {
    statistic.forEach(f::accept);
  }
}

