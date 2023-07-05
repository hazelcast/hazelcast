package com.hazelcast.jet.mongodb.impl;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorSupplier;
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

    public DbCheckingPMetaSupplier create() {
        return new DbCheckingPMetaSupplier(requiredPermission, shouldCheck, forceTotalParallelismOne, databaseName, collectionName, clientSupplier, dataConnectionRef, processorSupplier);
    }
}