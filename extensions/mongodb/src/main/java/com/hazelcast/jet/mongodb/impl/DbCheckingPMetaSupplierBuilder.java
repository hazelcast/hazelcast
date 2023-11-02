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
package com.hazelcast.jet.mongodb.impl;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.mongodb.client.MongoClient;

public class DbCheckingPMetaSupplierBuilder {
    private boolean checkResourceExistence;
    private boolean forceTotalParallelismOne;
    private String databaseName;
    private String collectionName;
    private SupplierEx<? extends MongoClient> clientSupplier;
    private DataConnectionRef dataConnectionRef;
    private ProcessorSupplier processorSupplier;
    private int preferredLocalParallelism = Vertex.LOCAL_PARALLELISM_USE_DEFAULT;

    public DbCheckingPMetaSupplierBuilder withCheckResourceExistence(boolean checkResourceExistence) {
        this.checkResourceExistence = checkResourceExistence;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder withForceTotalParallelismOne(boolean forceTotalParallelismOne) {
        this.forceTotalParallelismOne = forceTotalParallelismOne;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder withDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder withCollectionName(String collectionName) {
        this.collectionName = collectionName;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder withClientSupplier(SupplierEx<? extends MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder withDataConnectionRef(DataConnectionRef dataConnectionRef) {
        this.dataConnectionRef = dataConnectionRef;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder withProcessorSupplier(ProcessorSupplier processorSupplier) {
        this.processorSupplier = processorSupplier;
        return this;
    }

    /**
     * Sets preferred local parallelism. If {@link #forceTotalParallelismOne} is selected, this
     * method will have no effect.
     */
    public DbCheckingPMetaSupplierBuilder withPreferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = preferredLocalParallelism;
        return this;
    }

    public DbCheckingPMetaSupplier build() {
        return new DbCheckingPMetaSupplier(checkResourceExistence, forceTotalParallelismOne,
                databaseName, collectionName, clientSupplier, dataConnectionRef, processorSupplier,
                preferredLocalParallelism);
    }
}
