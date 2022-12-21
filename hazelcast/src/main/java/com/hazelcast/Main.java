package com.hazelcast;

import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;

public class Main {

    public static void main(String[] args) {
        TpcLogger logger = TpcLoggerLocator.getLogger(Main.class);
        logger.info("foobar");
    }
}
