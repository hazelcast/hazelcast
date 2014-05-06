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

package com.hazelcast.jca;

import java.io.PrintWriter;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Base class to allow simpler logging according to the JCA specs
 * and the Hazelcast Logging Framework
 */
public class JcaBase {
    /**
     * Class LOGGER from hazelcast's logging framework
     */
    private static final ILogger LOGGER = Logger.getLogger("com.hazelcast.jca");
    /**
     * Container's LOGGER
     */
    private PrintWriter logWriter;

    /**
     * Convenient method for {@link log(Level, String, null)}
     *
     * @param logLevel The level to log on
     * @param message  The message to be logged
     * @see #log(Level, String, Throwable)
     */
    void log(Level logLevel, String message) {
        log(logLevel, message, null);
    }

    /**
     * Logs the given message and throwable (if any) if the
     * configured logging level is set for the given one.
     * The message (and throwable) is logged to Hazelcast's logging
     * framework <b>and</b> the container specific print writer
     *
     * @param logLevel The level to log on
     * @param message  The message to be logged
     * @param t        The throwable to also log (message with stacktrace)
     */
    void log(Level logLevel, String message, Throwable t) {
        if (LOGGER.isLoggable(logLevel)) {
            //Log to hazelcast loggin framework itself
            LOGGER.log(logLevel, message, t);
            final PrintWriter logWriter = getLogWriter();
            //Log via the container if possible
            if (logWriter != null) {
                logWriter.write(message);
                if (t != null) {
                    t.printStackTrace(logWriter);
                }
            }
        }
    }

    /**
     * @return The container-specific LOGGER
     */
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    /**
     * Sets the container specific container LOGGER
     *
     * @param printWriter the new LOGGER to be used
     */
    public void setLogWriter(PrintWriter printWriter) {
        this.logWriter = printWriter;
    }
}
