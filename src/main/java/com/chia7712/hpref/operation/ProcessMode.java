package com.chia7712.hpref.operation;

import com.chia7712.hpref.util.EnumUtil;
import java.util.Optional;

public enum ProcessMode {
  ASYNC, SYNC, BUFFER, SHARED_BUFFER;
  public static Optional<ProcessMode> find(String value) {
    return EnumUtil.find(value, ProcessMode.class);
  }
}

