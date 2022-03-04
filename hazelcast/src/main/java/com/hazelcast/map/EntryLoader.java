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

package com.hazelcast.map;

import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * This is an extension to {@link MapLoader}. Entries loaded by EntryLoader
 * may have expiration times attached to them. In that case, the entry
 * is removed after given expiration time.
 *
 * See {@link MapLoader}.
 *
 * @param <K> type of the EntryLoader key
 * @param <V> type of the EnyryLoader value
 */
public interface EntryLoader<K, V> extends MapLoader<K, EntryLoader.MetadataAwareValue<V>> {

    /**
     * Represents a value with an expiration time attached to it.
     * Expiration time is optional.
     * @param <V> the type of the value object
     */
    class MetadataAwareValue<V> {

        /**
         * Represents no expiration time for a particular value
         */
        public static final long NO_TIME_SET = Long.MAX_VALUE;

        private final V value;

        private final long expirationTime;

        /**
         * Creates a value without attaching an expiration time
         * @param value the value
         */
        public MetadataAwareValue(V value) {
            this.value = isNotNull(value, "value");
            this.expirationTime = NO_TIME_SET;
        }

        /**
         * Creates a value and attaches an expiration time to it.
         * See {@link #getExpirationTime()} for how expiration time
         * is defined.
         *
         * @param value the value
         * @param expirationTime expiration time associated with the value
         */
        public MetadataAwareValue(V value, long expirationTime) {
            this.value = isNotNull(value, "value");
            this.expirationTime = expirationTime;
        }

        /**
         * Returns the value
         * @return the value
         */
        public V getValue() {
            return value;
        }

        /**
         * The expiration date of this entry. The entry is removed from
         * maps after the specified date. This value overrides any expiration
         * time calculated by using ttl and idle time configurations, both
         * per key and per map configurations.
         *
         * @return  the difference, measured in milliseconds, between
         *          the expiration time and midnight, January 1, 1970 UTC.
         */
        public long getExpirationTime() {
            return expirationTime;
        }
    }
}
