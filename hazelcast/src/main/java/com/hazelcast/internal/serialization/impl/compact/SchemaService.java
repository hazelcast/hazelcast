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

package com.hazelcast.internal.serialization.impl.compact;

/**
 * Service to put and get metadata to cluster.
 * <p>
 * Client implementations should make sure that even if cluster has restarted metadata will be put back.
 */
public interface SchemaService {

    String SERVICE_NAME = "schema-service";

    /**
     * Gets the schema with the given id either by
     * <ul>
     *     <li>returning it directly from the local registry, if it exists.</li>
     *     <li>searching the cluster.</li>
     * </ul>
     */
    Schema get(long schemaId);

    /**
     * Puts the schema with the given id to the cluster.
     */
    void put(Schema schema);

    /**
     * Puts the schema to only the local registry of the schema service.
     */
    void putLocal(Schema schema);
}
