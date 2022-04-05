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

/**
 * Service for logging {@link AuditableEvent AuditableEvents}. Standard events have their type identifiers defined as constants
 * in the {@link AuditlogTypeIds} class.
 * <p>
 * The invocations on this interface may be done in performance sensitive places so the implementation should not do anything
 * too heavy.
 */
public interface AuditlogService {

    /**
     * Logs given event.
     *
     * @param auditableEvent the event to be logged.
     */
    void log(AuditableEvent auditableEvent);

    /**
     * Creates and logs a simple parameterless message at the given level.
     *
     * @param level the log level
     * @param eventTypeId unique identifier of the event type
     * @param message the message to log
     */
    void log(String eventTypeId, Level level, String message);

    /**
     * Creates and logs a simple parameterless message with an associated throwable at the given level.
     *
     * @param message the message to log
     * @param eventTypeId unique identifier of the event type
     * @param thrown the Throwable associated to the message
     */
    void log(String eventTypeId, Level level, String message, Throwable thrown);

    /**
     * Returns an instance of the {@link EventBuilder} interface. It can be performance optimized (e.g. when Event Audit logging
     * is disabled). The {@link EventBuilder} allows creating {@link AuditableEvent auditable events} by using simple fluent
     * API.
     *
     * @param typeId Unique identifier for given type of auditable event.
     * @return {@link EventBuilder} instance
     */
    EventBuilder<?> eventBuilder(String typeId);
}
