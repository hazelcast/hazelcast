/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
 * Service to put and get meta data to cluster
 * <p>
 * Client implementations shpuld make sure that even if cluster has restarted meta data will be put back.
 */
public interface SchemaService {

    String SERVICE_NAME = "schema-service";

    Schema get(long schemaId);

    /**
     * Puts the schema to the cluster with the given id
     */
    void put(Schema schema);
}
