package com.hazelcast.util;

import com.hazelcast.logging.ILogger;

/**
 * Provides utility methods related to logging
 */
public final class LoggingUtil {

    private LoggingUtil() {
    }

    public static void logIfFinestEnabled(ILogger logger, String message) {
        if (logger.isFinestEnabled()) {
            logger.finest(message);
        }
    }
}
