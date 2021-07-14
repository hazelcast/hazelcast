/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;

/**
 * Utility methods to suppress building of messages for a disabled logging
 * level.
 */
public final class LoggingUtil {
    private LoggingUtil() {
    }

    public static void logFine(ILogger logger, String template, Object arg1) {
        if (logger.isFineEnabled()) {
            logger.fine(String.format(template, arg1));
        }
    }

    public static void logFine(ILogger logger, String template, Object arg1, Object arg2) {
        if (logger.isFineEnabled()) {
            logger.fine(String.format(template, arg1, arg2));
        }
    }

    public static void logFine(ILogger logger, String template, Object arg1, Object arg2, Object arg3) {
        if (logger.isFineEnabled()) {
            logger.fine(String.format(template, arg1, arg2, arg3));
        }
    }

    public static void logFine(ILogger logger, String template, Object arg1, Object arg2, Object arg3, Object arg4) {
        if (logger.isFineEnabled()) {
            logger.fine(String.format(template, arg1, arg2, arg3, arg4));
        }
    }

    public static void logFinest(ILogger logger, String template, Object arg1) {
        if (logger.isFinestEnabled()) {
            logger.finest(String.format(template, arg1));
        }
    }

    public static void logFinest(ILogger logger, String template, Object arg1, Object arg2) {
        if (logger.isFinestEnabled()) {
            logger.finest(String.format(template, arg1, arg2));
        }
    }

    public static void logFinest(ILogger logger, String template, Object arg1, Object arg2, Object arg3) {
        if (logger.isFinestEnabled()) {
            logger.finest(String.format(template, arg1, arg2, arg3));
        }
    }

    public static void logFinest(ILogger logger, String template, Object arg1, Object arg2, Object arg3, Object arg4) {
        if (logger.isFinestEnabled()) {
            logger.finest(String.format(template, arg1, arg2, arg3, arg4));
        }
    }
}
