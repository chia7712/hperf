package com.chia7712.hpref.operation;

import java.util.Objects;

public class Record implements Comparable<Record> {

  private final ProcessMode processMode;
  private final RequestMode requestMode;
  private final DataType dataType;
  private final String name;

  public Record(final ProcessMode processMode, final RequestMode requestMode,
    final DataType dataType) {
    this.processMode = processMode;
    this.requestMode = requestMode;
    this.dataType = dataType;
    this.name = new StringBuilder(processMode.name())
      .append("/")
      .append(requestMode.name())
      .append("/")
      .append(dataType.name())
      .toString();
  }

  public ProcessMode getProcessMode() {
    return processMode;
  }

  public RequestMode getRequestMode() {
    return requestMode;
  }

  public DataType getDataType() {
    return dataType;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof Record)) {
      return false;
    }
    return compareTo((Record) o) == 0;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 71 * hash + Objects.hashCode(this.processMode);
    hash = 71 * hash + Objects.hashCode(this.requestMode);
    hash = 71 * hash + Objects.hashCode(this.dataType);
    return hash;
  }

  @Override
  public int compareTo(Record o) {
    int rval = processMode.compareTo(o.getProcessMode());
    if (rval != 0) {
      return rval;
    }
    rval = requestMode.compareTo(o.getRequestMode());
    if (rval != 0) {
      return rval;
    }
    return dataType.compareTo(o.getDataType());
  }

  @Override
  public String toString() {
    return name;
  }

}
