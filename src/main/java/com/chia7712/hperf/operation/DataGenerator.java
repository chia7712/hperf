package com.chia7712.hperf.operation;

import com.chia7712.hperf.schedule.Dispatcher;
import com.chia7712.hperf.schedule.DispatcherFactory;
import com.chia7712.hperf.schedule.Packet;
import com.chia7712.hperf.view.Arguments;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
  private static final String NUMBER_OF_THREAD = "threads";
  private static final String TABLE_NAME = "table";
  private static final String NUMBER_OF_ROW = "rows";
  private static final String BATCH_SIZE = "batchSize";
  private static final String NUMBER_OF_QUALIFIER = "qualCount";
  private static final String QUALIFIER_SIZE = "qualifierSize";
  private static final String VALUE_SIZE = "valueSize";
  private static final String FLUSH_AT_THE_END = "flushtable";
  private static final String LARGE_QUALIFIER = "largequal";
  private static final String LOG_INTERVAL = "logInterval";
  private static final String RANDOM_ROW = "randomRow";

  public static void main(String[] args)
    throws IOException, InterruptedException, ExecutionException {
    Arguments arguments = new Arguments(Arrays.asList(NUMBER_OF_THREAD, TABLE_NAME, NUMBER_OF_ROW),
      Arrays.asList(ProcessMode.class.getSimpleName(), RequestMode.class.getSimpleName(),
        DataType.class.getSimpleName(), Durability.class.getSimpleName(), BATCH_SIZE,
        NUMBER_OF_QUALIFIER, VALUE_SIZE, FLUSH_AT_THE_END, LARGE_QUALIFIER, LOG_INTERVAL,
        RANDOM_ROW), Arrays
      .asList(getDescription(ProcessMode.class.getSimpleName(), ProcessMode.values()),
        getDescription(RequestMode.class.getSimpleName(), RequestMode.values()),
        getDescription(DataType.class.getSimpleName(), DataType.values()),
        getDescription(Durability.class.getSimpleName(), Durability.values())));
    arguments.validate(args);
    final int threads = arguments.getInt(NUMBER_OF_THREAD);
    final TableName tableName = TableName.valueOf(arguments.get(TABLE_NAME));
    final long totalRows = arguments.getLong(NUMBER_OF_ROW);
    final Optional<ProcessMode> processMode =
      ProcessMode.find(arguments.get(ProcessMode.class.getSimpleName()));
    final Optional<RequestMode> requestMode =
      RequestMode.find(arguments.get(RequestMode.class.getSimpleName()));
    final Optional<DataType> dataType =
      DataType.find(arguments.get(DataType.class.getSimpleName()));
    final Durability durability = arguments.get(Durability.class, Durability.USE_DEFAULT);
    final int batchSize = arguments.getInt(BATCH_SIZE, 100);
    final int qualCount = arguments.getInt(NUMBER_OF_QUALIFIER, 1);
    final Set<byte[]> families = findColumn(tableName);
    final int qualSize = arguments.getInt(QUALIFIER_SIZE, 5);
    final int valueSize = arguments.getInt(VALUE_SIZE, 10);
    final boolean needFlush = arguments.getBoolean(FLUSH_AT_THE_END, true);
    final int logInterval = arguments.getInt(LOG_INTERVAL, 5);
    final boolean randomRow = arguments.getBoolean(RANDOM_ROW, false);
    ExecutorService service = Executors.newFixedThreadPool(threads,
      Threads.newDaemonThreadFactory("-" + DataGenerator.class.getSimpleName()));
    Dispatcher dispatcher = DispatcherFactory.get(totalRows, batchSize);
    DataStatistic statistic = new DataStatistic();
    List<Statisticable> statisticables = new ArrayList<>(threads);
    try (ConnectionWrap conn = new ConnectionWrap(processMode, requestMode,
      needFlush ? tableName : null)) {
      List<CompletableFuture> slaves = new ArrayList<>(threads);
      Map<SlaveCatalog, AtomicInteger> slaveCatalog = new TreeMap<>();
      for (int i = 0; i != threads; ++i) {
        Slave slave = conn.createSlave(tableName, statistic, batchSize);
        statisticables.add(slave);
        LOG.info("Starting #" + i + " " + slave);
        slaveCatalog.computeIfAbsent(new SlaveCatalog(slave), k -> new AtomicInteger(0))
          .incrementAndGet();
        CompletableFuture fut = CompletableFuture.runAsync(() -> {
          Optional<Packet> packet;
          RowWork.Builder builder =
            RowWork.newBuilder().setDurability(durability).setFamilies(families)
              .setQualifierSize(qualSize).setValueSize(valueSize).setQualifierCount(qualCount)
              .setRandomRow(randomRow);
          try {
            while ((packet = dispatcher.getPacket()).isPresent()) {
              while (packet.get().hasNext()) {
                long next = packet.get().next();
                slave
                  .updateRow(builder.setBatchType(getDataType(dataType)).setRowIndex(next).build());
              }
              packet.get().commit();
            }
          } catch (IOException | InterruptedException ex) {
            LOG.error("Failed to operate the row update", ex);
          } finally {
            try {
              slave.close();
            } catch (Exception ex) {
              LOG.error("Failed to close the slave", ex);
            }
          }

        }, service);
        slaves.add(fut);
      }
      AtomicBoolean stop = new AtomicBoolean(false);
      CompletableFuture logger = CompletableFuture.runAsync(() -> {
        final long startTime = System.currentTimeMillis();
        long maxThroughput = 0;
        try {
          while (!stop.get()) {
            maxThroughput = log(statistic, totalRows, startTime, maxThroughput, statisticables);
            TimeUnit.SECONDS.sleep(logInterval);
          }
        } catch (InterruptedException ex) {
          LOG.error("The logger is interrupted", ex);
        } finally {
          log(statistic, totalRows, startTime, maxThroughput, statisticables);
        }
      });
      slaves.forEach(CompletableFuture::join);
      stop.set(true);
      logger.join();
      LOG.info("threads:" + threads + ", tableName:" + tableName + ", totalRows:" + totalRows + ", "
        + ProcessMode.class.getSimpleName() + ":" + processMode + ", " + RequestMode.class
        .getSimpleName() + ":" + requestMode + ", " + DataType.class.getSimpleName() + ":"
        + dataType + ", " + Durability.class.getSimpleName() + ":" + durability + ", batchSize:"
        + batchSize + ", qualCount:" + qualCount);
      slaveCatalog.forEach((k, v) -> LOG.info(k + " " + v));
    }
  }

  private static class SlaveCatalog implements Comparable<SlaveCatalog> {

    private final ProcessMode processMode;
    private final RequestMode requestMode;

    SlaveCatalog(Slave slave) {
      this(slave.getProcessMode(), slave.getRequestMode());
    }

    SlaveCatalog(final ProcessMode processMode, RequestMode requestMode) {
      this.processMode = processMode;
      this.requestMode = requestMode;
    }

    @Override
    public int compareTo(SlaveCatalog o) {
      int rval = processMode.compareTo(o.processMode);
      if (rval != 0) {
        return rval;
      }
      return requestMode.compareTo(o.requestMode);
    }

    @Override
    public String toString() {
      return processMode + "/" + requestMode;
    }
  }

  private static String toString(List<Statisticable> statisticables) {
    StringBuilder builder = new StringBuilder();
    statisticables.stream().mapToLong(Statisticable::getProcessedRows).sorted()
      .forEach(count -> builder.append(count).append(","));
    return builder.length() > 0 ? builder.substring(0, builder.length() - 1) : builder.toString();
  }

  private static long log(DataStatistic statistic, long totalRows, long startTime,
    long maxThroughput, List<Statisticable> statisticables) {
    long elapsed = (System.currentTimeMillis() - startTime) / 1000;
    long committedRows = statistic.getCommittedRows();
    if (elapsed <= 0 || committedRows <= 0) {
      return maxThroughput;
    }
    long throughput = committedRows / elapsed;
    if (throughput <= 0) {
      return maxThroughput;
    }
    maxThroughput = Math.max(maxThroughput, throughput);
    LOG.info("------------------------");
    LOG.info(toString(statisticables));
    LOG.info("total rows:" + totalRows);
    LOG.info("max throughput(rows/s):" + maxThroughput);
    LOG.info("throughput(rows/s):" + throughput);
    LOG.info("remaining(s):" + (totalRows - committedRows) / throughput);
    LOG.info("elapsed(s):" + elapsed);
    LOG.info("committed(rows):" + committedRows);
    LOG.info("processing(rows):" + statistic.getProcessingRows());
    statistic.consume((r, i) -> LOG.info(r.toString() + ":" + i));
    return maxThroughput;
  }

  private static DataType getDataType(Optional<DataType> type) {
    if (type.isPresent()) {
      return type.get();
    } else {
      int index = (int) (Math.random() * DataType.values().length);
      return DataType.values()[index];
    }
  }

  private static Set<byte[]> findColumn(TableName tableName) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(); Admin admin = conn.getAdmin()) {
      HTableDescriptor desc = admin.getTableDescriptor(tableName);
      Set<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      for (HColumnDescriptor col : desc.getColumnFamilies()) {
        LOG.info("find column:" + col.getNameAsString());
        columns.add(col.getName());
      }
      return columns;
    }
  }

  public static String getDescription(String name, Enum[] ops) {
    StringBuilder builder = new StringBuilder(name + ":");
    for (Enum op : ops) {
      builder.append(op.name()).append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }

  private static class ConnectionWrap implements Closeable {

    private final ProcessMode processMode;
    private final RequestMode requestMode;
    private final Connection conn;
    private final TableName nameToFlush;
    private BufferedMutator bm;

    ConnectionWrap(Optional<ProcessMode> processMode, Optional<RequestMode> requestMode,
      TableName nameToFlush) throws IOException {
      this.processMode = processMode.orElse(null);
      this.requestMode = requestMode.orElse(null);
      this.nameToFlush = nameToFlush;
      if (processMode.isPresent()) {
        switch (processMode.get()) {
          case SYNC:
            conn = ConnectionFactory.createConnection();
            break;
          case BUFFER:
          case SHARED_BUFFER:
            conn = ConnectionFactory.createConnection();
            break;
          default:
            throw new IllegalArgumentException("Unknown type:" + processMode.get());
        }
      } else {
        conn = ConnectionFactory.createConnection();
      }
    }

    private ProcessMode getProcessMode() {
      if (processMode != null) {
        return processMode;
      } else {
        int index = (int) (Math.random() * ProcessMode.values().length);
        return ProcessMode.values()[index];
      }
    }

    private RequestMode getRequestMode() {
      if (requestMode != null) {
        return requestMode;
      } else {
        int index = (int) (Math.random() * RequestMode.values().length);
        return RequestMode.values()[index];
      }
    }

    Slave createSlave(final TableName tableName, final DataStatistic statistic, final int batchSize)
      throws IOException {
      ProcessMode p = getProcessMode();
      RequestMode r = getRequestMode();

      switch (p) {
        case SYNC:
          switch (r) {
            case BATCH:
              return new BatchSlaveSync(conn.getTable(tableName), statistic, batchSize);
            case NORMAL:
              return new NormalSlaveSync(conn.getTable(tableName), statistic, batchSize);
          }
          break;
        case BUFFER:
          return new BufferSlaveSync(conn.getBufferedMutator(tableName), statistic, batchSize,
            true);
        case SHARED_BUFFER:
          if (bm == null) {
            bm = conn.getBufferedMutator(tableName);
          }
          return new BufferSlaveSync(bm, statistic, batchSize, false);
      }
      throw new RuntimeException(
        "Failed to find the suitable slave. ProcessMode:" + p + ", RequestMode:" + r);
    }

    @Override
    public void close() {
      flush();
      safeClose(bm);
      safeClose(conn);
    }

    private void flush() {
      if (nameToFlush == null) {
        return;
      }
      try (Admin admin = conn.getAdmin()) {
        admin.flush(nameToFlush);
      } catch (IOException ex) {
        LOG.error("Failed to flush the data", ex);
      }
    }

    private static void safeClose(Closeable obj) {
      if (obj != null) {
        try {
          obj.close();
        } catch (IOException ex) {
          LOG.error("Failed to close " + obj.getClass().getName(), ex);
        }
      }
    }
  }

}

