/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cdc;

import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.impl.DebeziumConfig;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for creating change data capture sources
 *
 * @since 4.2
 */
@EvolvingApi
public final class DebeziumCdcSources {

    private DebeziumCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from your Debezium
     * supported database to the Hazelcast Jet pipeline.
     *
     * @param name       name of this source, needs to be unique, will be
     *                   passed to the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also
     * to construct the source once configuration is done
     */
    @Nonnull
    public static Builder debezium(@Nonnull String name, @Nonnull String connectorClass) {
        return new Builder(name, connectorClass);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from any Debezium supported database to Hazelcast Jet.
     */
    public static final class Builder {

        private final DebeziumConfig config;

        /**
         * @param name           name of the source, needs to be unique,
         *                       will be passed to the underlying Kafka
         *                       Connect source
         * @param connectorClass name of the Java class for the connector,
         *                       hardcoded for each type of DB
         */
        private Builder(String name, String connectorClass) {
            config = new DebeziumConfig(name, connectorClass);
        }

        /**
         * Can be used to set any property supported by Debezium.
         */
        @Nonnull
        public Builder setProperty(@Nonnull String key, @Nonnull String value) {
            config.setProperty(key, value);
            return this;
        }

        /**
         * Returns an actual source based on the properties set so far.
         */
        @Nonnull
        public StreamSource<ChangeRecord> build() {
            return config.createSource();
        }
    }

}
