package com.chia7712.hperf.operation;

import com.chia7712.hperf.util.EnumUtil;
import java.util.Optional;

public enum RequestMode {
  BATCH, NORMAL;
  public static Optional<RequestMode> find(String value) {
    return EnumUtil.find(value, RequestMode.class);
  }
}