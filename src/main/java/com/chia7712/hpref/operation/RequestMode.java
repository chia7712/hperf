package com.chia7712.hpref.operation;

import com.chia7712.hpref.util.EnumUtil;
import java.util.Optional;

public enum RequestMode {
  BATCH, NORMAL;
  public static Optional<RequestMode> find(String value) {
    return EnumUtil.find(value, RequestMode.class);
  }
}