/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.logging.ILogger;

import java.time.Instant;
import java.util.Map;

/**
 * Allows to suppress (= ignore) logging actions based on parameters.
 * Useful when you need to silence a frequent log action, such as repeated exception.
 */
public class FrequentLogSuppressor {

    private final Map<String, LoggedException> errors;

    private final int logPeriod;
    private final int threshold;
    private final ILogger logger;

    /**
     * Create FrequentLogSuppressor
     *
     * @param logPeriod period how often a suppressed log action is run, despite being suppressed
     * @param threshold threshold for a log action becoming suppressed
     * @param logger    logger used to print additional info about suppressed log actions
     */
    public FrequentLogSuppressor(int logPeriod, int threshold, ILogger logger) {
        this.logPeriod = logPeriod;
        this.threshold = threshold;
        this.logger = logger;
        // Use Map with weakly referenced keys
        this.errors = new ConcurrentReferenceHashMap<>();
    }

    /**
     * Constructor for tests - uses provided Map instead of ConcurrentReferenceHashMap
     */
    FrequentLogSuppressor(int frequentLogPeriod, int threshold, ILogger logger,
                          Map<String, LoggedException> errors) {
        this.logPeriod = frequentLogPeriod;
        this.threshold = threshold;
        this.logger = logger;
        this.errors = errors;
    }

    /**
     * Runs the given log action, or ignores it based on the
     * `frequentLogPeriod` and `suppressLimit` parameters.
     * The log action is first called N times up to the
     * `suppressLimit`, then it is ignored and called once
     * per `frequentLogPeriod`.
     */
    public void runSuppressed(Throwable e, Runnable logAction) {
        errors.compute(ExceptionUtil.toString(e), (key, record) -> {
            if (record == null) {
                logAction.run();
                return new LoggedException();
            } else {
                record.inc();
                if (record.total < threshold) {
                    record.resetLastLog();
                    logAction.run();
                } else if (record.total == threshold) {
                    logger.warning("Frequent log operation detected, future occurrences will be suppressed and only "
                                   + "logged every " + logPeriod + " seconds");

                    record.resetLastLog();
                    logAction.run();
                } else if (record.lastLogTime.plusSeconds(logPeriod).isBefore(Instant.now())) {
                    logger.warning("The following suppressed log had " + record.sinceLastLog
                                   + " occurrences since last log, " + record.total + " in total");
                    logAction.run();
                    record.resetLastLog();
                }
                return record;
            }
        });
    }

    private static class LoggedException {

        // total number of occurrences since the beginning
        int total;
        // number of occurrences since the last log
        int sinceLastLog;
        Instant lastLogTime;

        LoggedException() {
            this.total = 1;
            this.sinceLastLog = 0;
            this.lastLogTime = Instant.now();
        }

        /**
         * Increment occurrence counters
         */
        void inc() {
            total++;
            sinceLastLog++;
        }

        /**
         * Reset the lastLogTime to `now` and number of occurrences since last log to 0
         */
        public void resetLastLog() {
            lastLogTime = Instant.now();
            sinceLastLog = 0;
        }
    }
}
