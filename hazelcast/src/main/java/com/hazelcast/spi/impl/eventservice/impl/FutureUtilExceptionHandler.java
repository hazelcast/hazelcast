package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.FutureUtil;

import java.util.concurrent.ExecutionException;

public final class FutureUtilExceptionHandler implements FutureUtil.ExceptionHandler {

    private final ILogger logger;
    private final String message;

    public FutureUtilExceptionHandler(ILogger logger, String message) {
        this.logger = logger;
        this.message = message;
    }

    @Override
    public void handleException(Throwable throwable) {
        if (throwable instanceof MemberLeftException) {
            logger.finest(message, throwable);
        } else if (throwable instanceof ExecutionException) {
            throw new HazelcastException(throwable);
        }
    }
}
