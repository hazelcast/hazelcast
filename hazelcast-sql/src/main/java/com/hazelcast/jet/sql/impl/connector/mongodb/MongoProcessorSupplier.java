/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.mongodb.client.MongoClient;

public abstract class MongoProcessorSupplier implements ProcessorSupplier {
    protected transient SupplierEx<? extends MongoClient> clientSupplier;
    protected final String databaseName;
    protected final String collectionName;
    protected final String[] externalNames;
    protected final String connectionString;
    protected final String dataConnectionName;

    protected final boolean forceMongoParallelismOne;
    protected final boolean checkExistenceOnEachConnect;

    protected MongoProcessorSupplier(MongoTable table) {
        this.connectionString = table.connectionString;
        this.dataConnectionName = table.dataConnectionName;
        this.databaseName = table.databaseName;
        this.collectionName = table.collectionName;
        this.forceMongoParallelismOne = table.isForceMongoParallelismOne();

        this.externalNames = table.externalNames();
        this.checkExistenceOnEachConnect = table.checkExistenceOnEachConnect();
    }
}
