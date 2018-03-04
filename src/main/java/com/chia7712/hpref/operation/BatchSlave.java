package com.chia7712.hpref.operation;

import com.chia7712.hpref.data.RandomData;
import com.chia7712.hpref.data.RandomDataFactory;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IndividualBytesFieldCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class BatchSlave implements Slave {
  private static final int LONG_LENGTH = String.valueOf(Long.MAX_VALUE).length();
  private static final RandomData RANDOM = RandomDataFactory.create();
  private static final List<String> KEYS =
    Arrays.asList("0-", "1-", "2-", "3-", "4-", "5-", "6-", "7-", "8-", "9-");
  private static final List<byte[]> KEYS_BYTES =
    KEYS.stream().map(Bytes::toBytes).collect(Collectors.toList());
  private static final byte[] DELIMITER = Bytes.toBytes("-");
  private final LongAdder processingRows = new LongAdder();
  private final LongAdder processedRows = new LongAdder();
  private final DataStatistic statistic;
  private final int batchRows;
  private final ConcurrentMap<DataType, Record> recordCache = new ConcurrentHashMap<>();

  public BatchSlave(final DataStatistic statistic, final int batchRows) {
    this.statistic = statistic;
    this.batchRows = batchRows;
  }

  @Override
  public long getProcessedRows() {
    return processedRows.longValue();
  }

  protected boolean needFlush() {
    return getProcessingRows() >= batchRows;
  }

  private void addNewRows(Record record, int delta) {
    assert delta >= 0;
    statistic.addNewRows(record, delta);
    processedRows.add(delta);
    processingRows.add(delta);
  }

  private Map<DataType, Record> getCache() {
    if (recordCache.size() == DataType.values().length) {
      return recordCache;
    }
    for (DataType type : DataType.values()) {
      recordCache.computeIfAbsent(type, k -> new Record(getProcessMode(), getRequestMode(), k));
    }
    return recordCache;
  }

  protected void finishRows(List<? extends Row> rows) {
    Map<DataType, Record> cache = getCache();
    processingRows.add(-rows.size());
    for (Row row : rows) {
      for (DataType type : DataType.values()) {
        if (type.isInstance(row)) {
          statistic.finishRows(cache.get(type), 1);
          break;
        }
      }
    }
  }

  protected void finishRows(DataType expectedType, int delta) {
    if (delta <= 0) {
      return;
    }
    Map<DataType, Record> cache = getCache();
    processingRows.add(-delta);
    statistic.finishRows(cache.get(expectedType), delta);
  }

  @Override
  public long getProcessingRows() {
    return processingRows.longValue();
  }

  protected Row prepareRow(RowWork work) {
    Row row;
    switch (work.getDataType()) {
      case PUT:
        row = createRandomPut(work);
        break;
      case DELETE:
        row = createRandomDelete(work);
        break;
      case GET:
        row = createRandomGet(work);
        break;
      case INCREMENT:
        row = createRandomIncrement(work);
        break;
      default:
        throw new RuntimeException("Unknown type:" + work.getDataType());
    }
    addNewRows(new Record(getProcessMode(), getRequestMode(), work.getDataType()), 1);
    return row;
  }

  @Override
  public String toString() {
    return this.getProcessMode() + "/" + this.getRequestMode();
  }

  @VisibleForTesting
  static byte[] createRow(RowWork work) {
    return work.getRandomRow() ? createRandomRow(work) : createNormalRow(work);
  }

  private static String formatIndex(long index) {
    StringBuilder finalIndex = new StringBuilder(String.valueOf(Math.abs(index)));
    while (LONG_LENGTH - finalIndex.length() > 0) {
      finalIndex.insert(0, "0");
    }
    return finalIndex.toString();
  }

  static byte[] createNormalRow(RowWork work) {
    byte[] key = KEYS_BYTES.get((int) (Math.random() * KEYS_BYTES.size()));
    byte[] rowIndexBytes = Bytes.toBytes(formatIndex(work.getRowIndex()));
    byte[] buf = new byte[key.length + rowIndexBytes.length];
    int offset = 0;
    offset = Bytes.putBytes(buf, offset, key, 0, key.length);
    offset = Bytes.putBytes(buf, offset, rowIndexBytes, 0, rowIndexBytes.length);
    return buf;
  }

  static byte[] createRandomRow(RowWork work) {
    byte[] key = KEYS_BYTES.get((int) (Math.random() * KEYS_BYTES.size()));
    byte[] rowIndexBytes = Bytes.toBytes(formatIndex(work.getRowIndex()));
    byte[] timeBytes = Bytes.toBytes(String.valueOf(System.currentTimeMillis()));
    byte[] randomIndex = Bytes.toBytes(formatIndex(RANDOM.getLong()));
    byte[] buf = new byte[key.length + timeBytes.length + DELIMITER.length + randomIndex.length
      + DELIMITER.length + rowIndexBytes.length];
    int offset = 0;
    offset = Bytes.putBytes(buf, offset, key, 0, key.length);
    offset = Bytes.putBytes(buf, offset, timeBytes, 0, timeBytes.length);
    offset = Bytes.putBytes(buf, offset, DELIMITER, 0, DELIMITER.length);
    offset = Bytes.putBytes(buf, offset, randomIndex, 0, randomIndex.length);
    offset = Bytes.putBytes(buf, offset, DELIMITER, 0, DELIMITER.length);
    offset = Bytes.putBytes(buf, offset, rowIndexBytes, 0, rowIndexBytes.length);
    return buf;
  }

  private static Put createRandomPut(RowWork work) {
    byte[] row = createRow(work);
    Map<byte[], List<Cell>> fmCells =
      createCells(row, work, () -> RANDOM.getBytes(work.getValueSize()));
    SimplePut put = new SimplePut(row);
    fmCells
      .forEach((fm, cells) -> cells.forEach(cell -> put.add(fm, cell, work.getQualifierCount())));
    return put;
  }

  private static Get createRandomGet(RowWork work) {
    byte[] row = createRow(work);
    Get get = new Get(row);
    switch (RANDOM.getInteger(1)) {
      case 0:
        for (byte[] family : work.getFamilies()) {
          for (int i = 0; i != work.getQualifierCount(); ++i) {
            get.addColumn(family, Bytes.toBytes(RANDOM.getLong()));
          }
        }
        break;
      default:
        for (byte[] family : work.getFamilies()) {
          get.addFamily(family);
        }
        break;
    }
    return get;
  }

  private static Delete createRandomDelete(RowWork work) {
    byte[] row = createRow(work);
    Map<byte[], List<Cell>> fmCells = createCells(row, work, () -> null);
    SimpleDelete delete = new SimpleDelete(row);
    fmCells.forEach(
      (fm, cells) -> cells.forEach(cell -> delete.add(fm, cell, work.getQualifierCount())));
    return delete;
  }

  private static Increment createRandomIncrement(RowWork work) {
    byte[] row = createRow(work);
    Map<byte[], List<Cell>> fmCells = createCells(row, work, () -> Bytes.toBytes(1L));
    SimpleIncrement inc = new SimpleIncrement(row);
    fmCells
      .forEach((fm, cells) -> cells.forEach(cell -> inc.add(fm, cell, work.getQualifierCount())));
    return inc;
  }

  private static Map<byte[], List<Cell>> createCells(byte[] row, RowWork work,
    Supplier<byte[]> valueGenerator) {
    Map<byte[], List<Cell>> rval = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    SimpleIncrement inc = new SimpleIncrement(row);
    inc.setDurability(work.getDurability());
    CellRewriter rewriter = null;
    byte[] value = valueGenerator.get();
    for (byte[] family : work.getFamilies()) {
      List<Cell> cells = rval.get(family);
      if (cells == null) {
        cells = new ArrayList<>();
        rval.put(family, cells);
      }
      for (int i = 0; i != work.getQualifierCount(); ++i) {
        Cell cell;
        byte[] qualifier = RANDOM.getBytes(work.getQualifierSize());
        if (rewriter == null) {
          cell = new IndividualBytesFieldCell(row, family, qualifier, HConstants.LATEST_TIMESTAMP,
            KeyValue.Type.Put, value);
          rewriter = CellRewriter.newCellRewriter(cell);
        } else {
          cell = rewriter.rewrite(CellRewriter.Field.FAMILY, family)
            .rewrite(CellRewriter.Field.QUALIFIER, qualifier)
            .getAndReset();
        }
        cells.add(cell);
      }
    }
    return rval;
  }

  private static class SimplePut extends Put {
    private Long heapSize;

    private SimplePut(byte[] row) {
      super(row);
    }

    @Override
    public long heapSize() {
      if (heapSize == null) {
        heapSize = super.heapSize();
      }
      return heapSize;
    }

    private SimplePut add(byte[] family, Cell cell, int expectedSize) {
      List<Cell> cells = familyMap.get(family);
      if (cells == null) {
        cells = new ArrayList<>(expectedSize);
        familyMap.put(family, cells);
      }
      cells.add(cell);
      return this;
    }
  }

  private static class SimpleDelete extends Delete {

    private SimpleDelete(byte[] row) {
      super(row);
    }

    private SimpleDelete add(byte[] family, Cell cell, int expectedSize) {
      List<Cell> cells = familyMap.get(family);
      if (cells == null) {
        cells = new ArrayList<>(expectedSize);
        familyMap.put(family, cells);
      }
      cells.add(cell);
      return this;
    }
  }

  private static class SimpleIncrement extends Increment {

    private SimpleIncrement(byte[] row) {
      super(row);
    }

    private SimpleIncrement add(byte[] family, Cell cell, int expectedSize) {
      List<Cell> cells = familyMap.get(family);
      if (cells == null) {
        cells = new ArrayList<>(expectedSize);
        familyMap.put(family, cells);
      }
      cells.add(cell);
      return this;
    }
  }
}

