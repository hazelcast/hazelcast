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

package com.hazelcast.auditlog;

import java.util.Map;

/**
 * Builder interface for {@link AuditableEvent} instances. The mandatory typeId is expected to be initiated by constructing the
 * builder.
 * <p>
 * <strong>Warning:</strong> The {@link #message(String) text message} and {@link #addParameter(String, Object) object
 * parameters} may provide more details about the event, but Hazelcast doesn't guarantee their content (e.g. exact text value of
 * the message, parameter names, object types used in parameters). They can change between versions.
 *
 * @see AuditlogService#eventBuilder(String)
 * @param <T> builder type
 */
public interface EventBuilder<T extends EventBuilder<T>> {

    /**
     * @param message event message. May be {@code null}
     * @return this instance
     */
    T message(String message);

    /**
     * @param parameters event parameters. Must not be {@code null}
     * @return this instance
     */
    T parameters(Map<String, Object> parameters);

    /**
     * Adds single event parameter to the parameters Map.
     * @param key Must not be {@code null}
     * @param value
     * @return this instance
     */
    T addParameter(String key, Object value);

    /**
     * Sets the event {@link Level}. The default value is {@link Level#INFO}.
     * @param level
     * @return this instance
     */
    T level(Level level);

    /**
     * Sets error/exception which caused the event (if any).
     * @param throwable
     * @return this instance
     */
    T cause(Throwable throwable);

    /**
     * If provided value greater than zero, then it sets the event timestamp explicitly. Otherwise the timestamp is filled by
     * calling {@link #build()} method.
     *
     * @param throwable
     * @return this instance
     */
    T timestamp(long timestamp);

    /**
     * Builds the {@link AuditableEvent} instance. If the timestamp is not configured explicitly, then the call sets event
     * timestamp too.
     *
     * @return not-{@code null} event instance ready to be logged
     */
    AuditableEvent build();

    /**
     * Builds the event and logs it to the service which provided this builder (if any).
     *
     * @see AuditlogService#eventBuilder(String)
     */
    void log();
}
