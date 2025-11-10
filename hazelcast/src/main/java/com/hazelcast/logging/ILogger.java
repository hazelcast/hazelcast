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

package com.hazelcast.logging;

import com.hazelcast.internal.tpcengine.logging.TpcLogger;

/**
 * The Hazelcast logging interface. It exists because Hazelcast doesn't want any dependencies on concrete logging frameworks, so
 * it creates its own meta logging framework behind which existing frameworks can be placed.
 *
 * @see AbstractLogger
 */
public interface ILogger extends TpcLogger {
    /**
     * Logs a LogEvent.
     *
     * @param logEvent the logEvent to log
     * @deprecated the method is unused
     */
    @Deprecated(since = "5.1")
    void log(LogEvent logEvent);

    /**
     * Logs to {@link #fine(String)} using a lazily evaluated {@code template} {@link String} with arguments, formatted using
     * {@link String#format(String, Object...)}
     *
     * @since 5.4
     */
    default void fine(String template, Object arg1) {
        if (isFineEnabled()) {
            fine(String.format(template, arg1));
        }
    }

    /** @see #fine(String, Object) */
    default void fine(String template, Object arg1, Object arg2) {
        if (isFineEnabled()) {
            fine(String.format(template, arg1, arg2));
        }
    }

    /** @see #fine(String, Object) */
    default void fine(String template, Object arg1, Object arg2, Object arg3) {
        if (isFineEnabled()) {
            fine(String.format(template, arg1, arg2, arg3));
        }
    }

    /** @see #fine(String, Object) */
    default void fine(String template, Object arg1, Object arg2, Object arg3, Object arg4) {
        if (isFineEnabled()) {
            fine(String.format(template, arg1, arg2, arg3, arg4));
        }
    }

    /** @see #fine(String, Object) */
    default void fine(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        if (isFineEnabled()) {
            fine(String.format(template, arg1, arg2, arg3, arg4, arg5));
        }
    }

    /** @see #fine(String, Object) */
    default void fine(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        if (isFineEnabled()) {
            fine(String.format(template, arg1, arg2, arg3, arg4, arg5, arg6));
        }
    }

    /**
     * Logs to {@link #finest(String)} using a lazily evaluated {@code template} {@link String} with arguments, formatted using
     * {@link String#format(String, Object...)}
     *
     * @since 5.4
     */
    default void finest(String template, Object arg1) {
        if (isFinestEnabled()) {
            finest(String.format(template, arg1));
        }
    }

    /** @see #finest(String, Object) */
    default void finest(String template, Object arg1, Object arg2) {
        if (isFinestEnabled()) {
            finest(String.format(template, arg1, arg2));
        }
    }

    /** @see #finest(String, Object) */
    default void finest(String template, Object arg1, Object arg2, Object arg3) {
        if (isFinestEnabled()) {
            finest(String.format(template, arg1, arg2, arg3));
        }
    }

    /** @see #finest(String, Object) */
    default void finest(String template, Object arg1, Object arg2, Object arg3, Object arg4) {
        if (isFinestEnabled()) {
            finest(String.format(template, arg1, arg2, arg3, arg4));
        }
    }

    /** @see #finest(String, Object) */
    default void finest(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        if (isFinestEnabled()) {
            finest(String.format(template, arg1, arg2, arg3, arg4, arg5));
        }
    }

    /** @see #finest(String, Object) */
    default void finest(String template, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        if (isFinestEnabled()) {
            finest(String.format(template, arg1, arg2, arg3, arg4, arg5, arg6));
        }
    }
}
