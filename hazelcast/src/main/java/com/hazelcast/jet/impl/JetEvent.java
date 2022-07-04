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

package com.hazelcast.jet.impl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

import static com.hazelcast.jet.impl.util.Util.toLocalTime;

/**
 * Holds a stream event and its timestamp. Jet processors receive and send
 * these objects, but the user's lambdas in the Pipeline API don't observe
 * them.
 *
 * @param <T> type of the wrapped event
 */
public final class JetEvent<T> {
    private final long timestamp;
    private final T payload;

    private JetEvent(long timestamp, @Nonnull T payload) {
        this.timestamp = timestamp;
        this.payload = payload;
    }

    /**
     * Creates a new {@code JetEvent} with the given components.
     */
    @Nullable
    public static <T> JetEvent<T> jetEvent(long timestamp, @Nullable T payload) {
        if (payload == null) {
            return null;
        }
        return new JetEvent<>(timestamp, payload);
    }

    /**
     * Returns the timestamp of this event.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Returns the wrapped event.
     */
    @Nonnull
    public T payload() {
        return payload;
    }

    @Override
    public String toString() {
        return "JetEvent{ts=" + toLocalTime(timestamp) + ", payload=" + payload + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JetEvent<?> jetEvent = (JetEvent<?>) o;
        return timestamp == jetEvent.timestamp && Objects.equals(payload, jetEvent.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, timestamp);
    }
}
