package com.hazelcast.cluster;

import com.hazelcast.util.Clock;

public class OffsetJumpingSystemClock extends Clock.ClockImpl {

    public static volatile int clockShiftOffset;

    @Override
    protected long currentTimeMillis() {
        return System.currentTimeMillis() + clockShiftOffset;
    }
}
