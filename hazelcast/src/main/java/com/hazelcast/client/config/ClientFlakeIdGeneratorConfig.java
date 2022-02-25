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

package com.hazelcast.client.config;

import com.hazelcast.flakeidgen.FlakeIdGenerator;

import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_PREFETCH_COUNT;
import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_PREFETCH_VALIDITY_MILLIS;
import static com.hazelcast.config.FlakeIdGeneratorConfig.MAXIMUM_PREFETCH_COUNT;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * The {@code ClientFlakeIdGeneratorConfig} contains the configuration for the client
 * regarding {@link com.hazelcast.core.HazelcastInstance#getFlakeIdGenerator(String)
 * Flake ID Generator}.
 */
public class ClientFlakeIdGeneratorConfig {

    private String name;
    private int prefetchCount = DEFAULT_PREFETCH_COUNT;
    private long prefetchValidityMillis = DEFAULT_PREFETCH_VALIDITY_MILLIS;

    // for spring-instantiation
    private ClientFlakeIdGeneratorConfig() {
    }

    public ClientFlakeIdGeneratorConfig(String name) {
        this.name = name;
    }

    /**
     * Copy-constructor
     */
    public ClientFlakeIdGeneratorConfig(ClientFlakeIdGeneratorConfig other) {
        this.name = other.name;
        this.prefetchCount = other.prefetchCount;
        this.prefetchValidityMillis = other.prefetchValidityMillis;
    }

    /**
     * Returns the configuration name. This can be actual object name or pattern.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name or name pattern for this config. Must not be modified after this
     * instance is added to {@link ClientConfig}.
     */
    public ClientFlakeIdGeneratorConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * @see #setPrefetchCount(int)
     */
    public int getPrefetchCount() {
        return prefetchCount;
    }

    /**
     * Sets how many IDs are pre-fetched on the background when one call to
     * {@link FlakeIdGenerator#newId()} is made. Default is 100.
     *
     * @param prefetchCount the desired prefetch count, in the range 1..100,000.
     * @return this instance for fluent API
     */
    public ClientFlakeIdGeneratorConfig setPrefetchCount(int prefetchCount) {
        checkTrue(prefetchCount > 0 && prefetchCount <= MAXIMUM_PREFETCH_COUNT,
                "prefetch-count must be 1.." + MAXIMUM_PREFETCH_COUNT + ", not " + prefetchCount);
        this.prefetchCount = prefetchCount;
        return this;
    }

    /**
     * @see #setPrefetchValidityMillis(long)
     */
    public long getPrefetchValidityMillis() {
        return prefetchValidityMillis;
    }

    /**
     * Sets for how long the pre-fetched IDs can be used. If this time elapses, a new batch of IDs will be
     * fetched. Time unit is milliseconds, default is 600,000 (10 minutes).
     * <p>
     * The IDs contain timestamp component, which ensures rough global ordering of IDs. If an ID
     * is assigned to an object that was created much later, it will be much out of order. If you don't care
     * about ordering, set this value to 0.
     *
     * @param prefetchValidityMs the desired ID validity or unlimited, if &lt;=0
     * @return this instance for fluent API
     */
    public ClientFlakeIdGeneratorConfig setPrefetchValidityMillis(long prefetchValidityMs) {
        this.prefetchValidityMillis = prefetchValidityMs;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientFlakeIdGeneratorConfig that = (ClientFlakeIdGeneratorConfig) o;

        if (prefetchCount != that.prefetchCount) {
            return false;
        }
        if (prefetchValidityMillis != that.prefetchValidityMillis) {
            return false;
        }
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + prefetchCount;
        result = 31 * result + (int) (prefetchValidityMillis ^ (prefetchValidityMillis >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ClientFlakeIdGeneratorConfig{"
                + "name='" + name + '\''
                + ", prefetchCount=" + prefetchCount
                + ", prefetchValidityMillis=" + prefetchValidityMillis
                + '}';
    }
}
