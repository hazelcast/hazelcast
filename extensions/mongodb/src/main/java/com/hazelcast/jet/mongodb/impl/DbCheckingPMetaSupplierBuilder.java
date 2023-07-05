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

import java.security.Permission;

public class DbCheckingPMetaSupplierBuilder {
    private Permission requiredPermission;
    private boolean shouldCheck;
    private boolean forceTotalParallelismOne;
    private String databaseName;
    private String collectionName;
    private SupplierEx<? extends MongoClient> clientSupplier;
    private DataConnectionRef dataConnectionRef;
    private ProcessorSupplier processorSupplier;
    private int preferredLocalParallelism = Vertex.LOCAL_PARALLELISM_USE_DEFAULT;

    public DbCheckingPMetaSupplierBuilder setRequiredPermission(Permission requiredPermission) {
        this.requiredPermission = requiredPermission;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder setShouldCheck(boolean shouldCheck) {
        this.shouldCheck = shouldCheck;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder setForceTotalParallelismOne(boolean forceTotalParallelismOne) {
        this.forceTotalParallelismOne = forceTotalParallelismOne;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder setCollectionName(String collectionName) {
        this.collectionName = collectionName;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder setClientSupplier(SupplierEx<? extends MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder setDataConnectionRef(DataConnectionRef dataConnectionRef) {
        this.dataConnectionRef = dataConnectionRef;
        return this;
    }

    public DbCheckingPMetaSupplierBuilder setProcessorSupplier(ProcessorSupplier processorSupplier) {
        this.processorSupplier = processorSupplier;
        return this;
    }

    /**
     * Sets preferred local parallelism. If {@link #forceTotalParallelismOne} is selected, this
     * method will have no effect.
     */
    public DbCheckingPMetaSupplierBuilder setPreferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = preferredLocalParallelism;
        return this;
    }

    public DbCheckingPMetaSupplier create() {
        return new DbCheckingPMetaSupplier(requiredPermission, shouldCheck, forceTotalParallelismOne, databaseName,
                collectionName, clientSupplier, dataConnectionRef, processorSupplier, preferredLocalParallelism);
    }
}
