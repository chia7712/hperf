package com.chia7712.hpref.util;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EnumUtil {
  private static final Logger LOG = LoggerFactory.getLogger(EnumUtil.class);
  public static <T extends Enum<T>> Optional<T> find(String value, Class<T> clz) {
    if (value == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(Enum.valueOf(clz, value.toUpperCase()));
    } catch (IllegalArgumentException e) {
      LOG.error("Unsupported enum:" + value, e);
      return Optional.empty();
    }
  }
  private EnumUtil(){}
}

