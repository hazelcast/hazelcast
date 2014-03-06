/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.logging;

import java.util.logging.Level;


/**
 * The Hazelcast logging interface. The reason if its existence is that Hazelcast doesn't want any dependencies
 * on concrete logging frameworks so it creates it own meta logging framework where existing logging frameworks can
 * be placed behind.
 */
public interface ILogger {

    /**
     * Logs a message at {@link Level#INFO}.
     *
     * @param message the message to log.
     */
    void info(String message);

    /**
     * Logs a message at {@link Level#FINEST}.
     *
     * @param message the message to log.
     */
    void finest(String message);

    /**
     * Logs a throwable at {@link Level#FINEST}.  The message of the Throwable will be the message.
     *
     * @param thrown the Throwable to log.
     */
    void finest(Throwable thrown);

    /**
     * Logs message with associated throwable information at {@link Level#FINEST}.
     *
     * @param message the message to log
     * @param thrown  the Throwable associated to the message.
     */
    void finest(String message, Throwable thrown);

    /**
     * Checks if the {@link Level#FINEST} is enabled.
     *
     * @return true if enabled, false otherwise.
     */
    boolean isFinestEnabled();

    /**
     * Logs a message at {@link Level#SEVERE}.
     *
     * @param message the message to log.
     */
    void severe(String message);

    /**
     * Logs a throwable at {@link Level#SEVERE}.  The message of the Throwable will be the message.
     *
     * @param thrown the Throwable to log.
     */
    void severe(Throwable thrown);

    /**
     * Logs message with associated throwable information at {@link Level#SEVERE}.
     *
     * @param message the message to log
     * @param thrown  the Throwable associated to the message.
     */
    void severe(String message, Throwable thrown);

    /**
     * Logs a message at {@link Level#WARNING}.
     *
     * @param message the message to log.
     */
    void warning(String message);

    /**
     * Logs a throwable at {@link Level#WARNING}.  The message of the Throwable will be the message.
     *
     * @param thrown the Throwable to log.
     */
    void warning(Throwable thrown);

    /**
     * Logs message with associated throwable information at {@link Level#WARNING}.
     *
     * @param message the message to log
     * @param thrown  the Throwable associated to the message.
     */
    void warning(String message, Throwable thrown);

    /**
     * Logs a message at the provided Level.
     *
     * @param level   the Level of logging.
     * @param message the message to log.
     */
    void log(Level level, String message);

    /**
     * Logs message with associated throwable information at the provided level.
     *
     * @param message the message to log
     * @param thrown  the Throwable associated to the message.
     */
    void log(Level level, String message, Throwable thrown);

    /**
     * Logs a LogEvent
     *
     * @param logEvent the logEvent to log.
     */
    void log(LogEvent logEvent);

    /**
     * Gets the logging Level.
     *
     * @return the logging Level.
     */
    Level getLevel();

    /**
     * Checks if a message at the provided level is going to be logged by this logger.
     *
     * @param level the log level.
     * @return true if this Logger will log messages for the provided level, false otherwise.
     */
    boolean isLoggable(Level level);
}
