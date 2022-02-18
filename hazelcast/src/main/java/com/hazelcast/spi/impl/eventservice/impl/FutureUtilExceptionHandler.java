/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.util.FutureUtil;

import java.util.concurrent.ExecutionException;

/**
 * Exception handler which logs the {@link #message} in case the exception is a {@link MemberLeftException},
 * otherwise wraps the exception in a {@link HazelcastException} and rethrows it.
 */
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
