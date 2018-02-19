package com.chia7712.hpref.operation;

import java.util.Set;
import org.apache.hadoop.hbase.client.Durability;

public class RowWork {
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private DataType type;
    private long rowIndex;
    private Set<byte[]> families;
    private int qualCount;
    private Durability durability;
    private int qualSize;
    private int valueSize;
    private boolean randomRow;

    public Builder setRandomRow(boolean randomRow) {
      this.randomRow = randomRow;
      return this;
    }

    public Builder setQualifierSize(int qualSize) {
      this.qualSize = qualSize;
      return this;
    }

    public Builder setValueSize(int valueSize) {
      this.valueSize = valueSize;
      return this;
    }

    public Builder setBatchType(DataType type) {
      this.type = type;
      return this;
    }

    public Builder setRowIndex(long rowIndex) {
      this.rowIndex = rowIndex;
      return this;
    }

    public Builder setFamilies(Set<byte[]> families) {
      this.families = families;
      return this;
    }

    public Builder setQualifierCount(int qualCount) {
      this.qualCount = qualCount;
      return this;
    }

    public Builder setDurability(Durability durability) {
      this.durability = durability;
      return this;
    }

    public RowWork build() {
      return new RowWork(type, rowIndex, families, qualCount, durability, qualSize, valueSize,
        randomRow);
    }

    private Builder() {
    }
  }

  private final DataType type;
  private final long rowIndex;
  private final Set<byte[]> families;
  private final int qualCount;
  private final Durability durability;
  private final int qualSize;
  private final int valueSize;
  private final boolean randomRow;

  private RowWork(final DataType type, final long rowIndex, final Set<byte[]> families,
    final int qualCount, final Durability durability, final int qualSize, final int valueSize,
    final boolean randomRow) {
    this.type = type;
    this.rowIndex = rowIndex;
    this.families = families;
    this.qualCount = qualCount;
    this.durability = durability;
    this.qualSize = qualSize;
    this.valueSize = valueSize;
    this.randomRow = randomRow;
  }

  public boolean getRandomRow() {
    return randomRow;
  }

  public int getQualifierSize() {
    return qualSize;
  }

  public int getValueSize() {
    return valueSize;
  }

  public DataType getDataType() {
    return type;
  }

  public long getRowIndex() {
    return rowIndex;
  }

  public Set<byte[]> getFamilies() {
    return families;
  }

  public int getQualifierCount() {
    return qualCount;
  }

  public Durability getDurability() {
    return durability;
  }
}
