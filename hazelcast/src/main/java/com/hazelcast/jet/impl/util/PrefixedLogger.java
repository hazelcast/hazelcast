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

package com.hazelcast.jet.impl.util;

import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogEvent;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import static com.hazelcast.jet.Util.idToString;

/**
 * An {@link ILogger} implementation that wraps another {@link ILogger} and
 * prefixes all the logged messages with a given prefix. The logger logs
 * the messages like: [prefix] message
 */
public class PrefixedLogger extends AbstractLogger {

    private final ILogger wrapped;
    private final String prefix;

    PrefixedLogger(ILogger wrapped, String prefix) {
        this.wrapped = wrapped;
        this.prefix = "[" + prefix + "] ";
    }

    public static ILogger prefixedLogger(ILogger logger, String prefix) {
        return new PrefixedLogger(logger, prefix);
    }

    public static String prefix(String jobName, long jobId, String vertexName) {
        return prefix(jobName, jobId, vertexName, null);
    }

    public static String prefix(String jobName, long jobId, String vertexName, int processorIndex) {
        return prefix(jobName, jobId, vertexName, "#" + processorIndex);
    }

    public static String prefix(String jobName, long jobId, String vertexName, String subClass) {
        String jobIdentification = jobName != null ? jobName : idToString(jobId);
        return jobIdentification + "/" + vertexName + (subClass == null ? "" : subClass);
    }

    @Override
    public void log(Level level, String message) {
        wrapped.log(level, prefix + message);
    }

    @Override
    public void log(Level level, String message, Throwable thrown) {
        wrapped.log(level, prefix + message, thrown);
    }

    @Override
    public void log(LogEvent logEvent) {
        LogRecord logRecord = logEvent.getLogRecord();
        logRecord.setMessage(prefix + logRecord.getMessage());
        wrapped.log(logEvent);
    }

    @Override
    public Level getLevel() {
        return wrapped.getLevel();
    }

    @Override
    public boolean isLoggable(Level level) {
        return wrapped.isLoggable(level);
    }
}
