package com.chia7712.hperf.operation;

import com.chia7712.hperf.util.EnumUtil;
import java.util.Optional;

public enum ProcessMode {
  ASYNC, SYNC, BUFFER, SHARED_BUFFER;
  public static Optional<ProcessMode> find(String value) {
    return EnumUtil.find(value, ProcessMode.class);
  }
}

