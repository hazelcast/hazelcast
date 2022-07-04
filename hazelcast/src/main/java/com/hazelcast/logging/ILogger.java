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

package com.hazelcast.logging;

import java.util.logging.Level;

/**
 * The Hazelcast logging interface. It exists because Hazelcast doesn't want any dependencies
 * on concrete logging frameworks, so it creates its own meta logging framework behind which
 * existing frameworks can be placed.
 *
 * @see AbstractLogger
 */
public interface ILogger {

    /**
     * Logs a message at the {@link Level#FINEST} level.
     *
     * @param message the message to log
     */
    void finest(String message);

    /**
     * Logs a throwable at the {@link Level#FINEST} level. The message of the
     * Throwable will be the logged message.
     *
     * @param thrown the Throwable to log
     */
    void finest(Throwable thrown);

    /**
     * Logs a message with an associated throwable at the {@link Level#FINEST} level.
     *
     * @param message the message to log
     * @param thrown  the Throwable associated to the message
     */
    void finest(String message, Throwable thrown);

    /**
     * Checks if the {@link Level#FINEST} level is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean isFinestEnabled();

    /**
     * Logs a message at the {@link Level#FINE} level.
     *
     * @param message the message to log
     */
    void fine(String message);

    /**
     * Logs a throwable at the {@link Level#FINE} level. The message of the
     * Throwable will be the logged message.
     *
     * @param thrown the Throwable to log
     */
    void fine(Throwable thrown);

    /**
     * Logs a message with an associated throwable at the {@link Level#FINE} level.
     *
     * @param message the message to log
     * @param thrown  the Throwable associated to the message
     */
    void fine(String message, Throwable thrown);

    /**
     * Checks if the {@link Level#FINE} level is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean isFineEnabled();

    /**
     * Logs a message at the {@link Level#INFO} level.
     *
     * @param message the message to log
     */
    void info(String message);

    /**
     * Logs a throwable at the {@link Level#INFO} level. The message of the
     * Throwable will be the logged message.
     *
     * @param thrown the Throwable to log
     */
    void info(Throwable thrown);

    /**
     * Logs a message with an associated throwable at the {@link Level#INFO} level.
     *
     * @param message the message to log
     * @param thrown  the Throwable associated to the message
     */
    void info(String message, Throwable thrown);

    /**
     * Checks if the {@link Level#INFO} level is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean isInfoEnabled();

    /**
     * Logs a message at the {@link Level#WARNING} level.
     *
     * @param message the message to log
     */
    void warning(String message);

    /**
     * Logs a throwable at the {@link Level#WARNING} level. The message of the
     * Throwable will be the logged message.
     *
     * @param thrown the Throwable to log
     */
    void warning(Throwable thrown);

    /**
     * Logs a message with an associated throwable at the {@link Level#WARNING} level.
     *
     * @param message the message to log
     * @param thrown  the Throwable associated to the message
     */
    void warning(String message, Throwable thrown);

    /**
     * Checks if the {@link Level#WARNING} is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean isWarningEnabled();

    /**
     * Logs a message at {@link Level#SEVERE}.
     *
     * @param message the message to log.
     */
    void severe(String message);

    /**
     * Logs a throwable at the {@link Level#SEVERE} level. The message of the
     * Throwable will be the logged message.
     *
     * @param thrown the Throwable to log
     */
    void severe(Throwable thrown);

    /**
     * Logs a message with an associated throwable at the {@link Level#SEVERE} level.
     *
     * @param message the message to log
     * @param thrown  the Throwable associated to the message
     */
    void severe(String message, Throwable thrown);

    /**
     * Checks if the {@link Level#SEVERE} is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean isSevereEnabled();

    /**
     * Logs a message at the given level.
     *
     * @param level   the log level
     * @param message the message to log
     */
    void log(Level level, String message);

    /**
     * Logs a message with an associated throwable at the given level.
     *
     * @param message the message to log
     * @param thrown  the Throwable associated to the message
     */
    void log(Level level, String message, Throwable thrown);

    /**
     * Logs a LogEvent.
     *
     * @param logEvent the logEvent to log
     * @deprecated Since 5.1, the method is unused
     */
    @Deprecated
    void log(LogEvent logEvent);

    /**
     * Gets the logging level.
     *
     * @return the logging level
     */
    Level getLevel();

    /**
     * Checks if a message at the given level is going to be logged by this logger.
     *
     * @param level the log level
     * @return true if this logger will log messages for the given level, false otherwise
     */
    boolean isLoggable(Level level);
}
