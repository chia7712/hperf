package com.chia7712.hpref.schedule;

import java.util.Optional;

public interface Dispatcher {

  long getDispatchedRows();

  long getCommittedRows();

  Optional<Packet> getPacket();

}

