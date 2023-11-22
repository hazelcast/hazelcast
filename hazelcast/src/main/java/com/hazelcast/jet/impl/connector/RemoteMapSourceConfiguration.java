/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for a remote map source
 *
 * @param <K> specifies key type
 * @param <V> specifies value type
 * @param <T> specifies emitted type
 */
public class RemoteMapSourceConfiguration<K, V, T> {

    private final String name;
    private final String dataConnectionName;
    private final ClientConfig clientConfig;
    private final Predicate<K, V> predicate;
    private final Projection<? super Map.Entry<K, V>, ? extends T> projection;

    public RemoteMapSourceConfiguration(String name,
                                        String dataConnectionName,
                                        ClientConfig clientConfig,
                                        Predicate<K, V> predicate,
                                        Projection<? super Map.Entry<K, V>, ? extends T> projection) {
        this.name = requireNonNull(name);
        this.dataConnectionName = dataConnectionName;
        this.clientConfig = clientConfig;
        this.predicate = predicate;
        this.projection = projection;
    }

    public boolean hasPredicate() {
        return predicate != null;
    }

    public String getName() {
        return name;
    }

    public String getDataConnectionName() {
        return dataConnectionName;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public Predicate<K, V> getPredicate() {
        return predicate;
    }

    public Projection<? super Map.Entry<K, V>, ? extends T> getProjection() {
        return projection;
    }

}
