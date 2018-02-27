/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline;

import javax.annotation.Nonnull;
import java.util.Objects;

import static com.hazelcast.jet.impl.util.Util.toLocalTime;

/**
 * Javadoc pending.
 */
public final class JetEventImpl<T> implements JetEvent<T> {
    private final T payload;
    private final long timestamp;

    private JetEventImpl(@Nonnull T payload, long timestamp) {
        this.timestamp = timestamp;
        this.payload = payload;
    }

    public static <T> JetEvent<T> jetEvent(T payload, long timestamp) {
        return new JetEventImpl<>(payload, timestamp);
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Nonnull
    @Override
    public T payload() {
        return payload;
    }

    @Override
    public String toString() {
        return "JetEvent{ts=" + toLocalTime(timestamp)
                + ", payload=" + payload
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JetEventImpl<?> jetEvent = (JetEventImpl<?>) o;
        return timestamp == jetEvent.timestamp &&
                Objects.equals(payload, jetEvent.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, timestamp);
    }
}
