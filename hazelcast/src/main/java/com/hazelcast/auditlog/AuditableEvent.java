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
 * Observable event in the system.
 */
public interface AuditableEvent {

    /**
     * @return Unique event type code. Must not be {@code null}.
     */
    String typeId();

    /**
     * Return a text description of the event.
     * <p>
     * <strong>Warning:</strong> Hazelcast doesn't guarantee content of the message. The value can change between versions.
     *
     * @return Event message
     */
    String message();

    /**
     * Return Map of parameters for given event.
     * <p>
     * <strong>Warning:</strong> Hazelcast doesn't guarantee content of the parameters Map (parameters, names, values
     * or types used). The value can change between versions.
     *
     * @return Event parameters. Must not be {@code null}.
     */
    Map<String, Object> parameters();

    /**
     * @return Importance level.  Must not be {@code null}.
     */
    Level level();

    /**
     * Return an exception or error (if any) which caused this event.
     *
     * @return event cause or {@code null}
     */
    Throwable cause();

    /**
     * Returns event timestamp as milliseconds from the epoch.
     *
     * @return event timestamp in millis
     */
    long getTimestamp();
}
