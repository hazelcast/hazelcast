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
import javax.annotation.Nullable;
import java.util.Objects;

import static com.hazelcast.jet.impl.util.Util.toLocalTime;

/**
 * Holds a stream event and its timestamp. Jet processors receive and send
 * these objects, but the user's lambdas in the Pipeline API don't observe
 * them.
 */
public final class JetEvent<T> {
    private final T payload;
    private final long timestamp;

    private JetEvent(@Nonnull T payload, long timestamp) {
        this.timestamp = timestamp;
        this.payload = payload;
    }

    @Nullable
    public static <T> JetEvent<T> jetEvent(@Nullable T payload, long timestamp) {
        if (payload == null) {
            return null;
        }
        return new JetEvent<>(payload, timestamp);
    }

    public long timestamp() {
        return timestamp;
    }

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
        return timestamp == jetEvent.timestamp &&
                Objects.equals(payload, jetEvent.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, timestamp);
    }
}
