package com.chia7712.hperf.schedule;

import java.util.Optional;

public interface Dispatcher {

  long getDispatchedRows();

  long getCommittedRows();

  Optional<Packet> getPacket();

}

