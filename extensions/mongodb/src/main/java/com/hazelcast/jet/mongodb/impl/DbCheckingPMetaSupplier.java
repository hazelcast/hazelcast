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

import com.hazelcast.cluster.Address;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.DataConnectionService;
import com.hazelcast.dataconnection.impl.InternalDataConnectionService;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.processor.ExpectNothingP;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.mongodb.dataconnection.MongoDataConnection;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.Util.arrayIndexOf;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.checkCollectionExists;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.checkDatabaseExists;
import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static java.util.Collections.singletonList;

/**
 * A {@link ProcessorMetaSupplier} that will check if requested database and collection exist before creating
 * the processors.
 */
public class DbCheckingPMetaSupplier implements ProcessorMetaSupplier {

    private final Permission requiredPermission;
    private final boolean shouldCheck;
    private boolean forceTotalParallelismOne;
    private final String databaseName;
    private final String collectionName;
    private final ProcessorSupplier processorSupplier;
    private final SupplierEx<? extends MongoClient> clientSupplier;
    private final DataConnectionRef dataConnectionRef;
    private int preferredLocalParallelism = Vertex.LOCAL_PARALLELISM_USE_DEFAULT;

    private transient Address ownerAddress;

    /**
     * Creates a new instance of this meta supplier.
     */
    public DbCheckingPMetaSupplier(@Nullable Permission requiredPermission,
                                   boolean shouldCheck,
                                   boolean forceTotalParallelismOne,
                                   @Nullable String databaseName,
                                   @Nullable String collectionName,
                                   @Nullable SupplierEx<? extends MongoClient> clientSupplier,
                                   @Nullable DataConnectionRef dataConnectionRef,
                                   @Nonnull ProcessorSupplier processorSupplier,
                                   int preferredLocalParallelism
    ) {
        this.requiredPermission = requiredPermission;
        this.shouldCheck = shouldCheck;
        this.forceTotalParallelismOne = forceTotalParallelismOne;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.processorSupplier = processorSupplier;
        this.clientSupplier = clientSupplier;
        this.dataConnectionRef = dataConnectionRef;
        this.preferredLocalParallelism = preferredLocalParallelism;
    }

    /**
     * Sets preferred local parallelism. If {@link #forceTotalParallelismOne} is selected, this
     * method will have no effect.
     */
    public DbCheckingPMetaSupplier withPreferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = forceTotalParallelismOne ? 1 : preferredLocalParallelism;
        return this;
    }

    @Override
    public int preferredLocalParallelism() {
        return forceTotalParallelismOne ? 1 : preferredLocalParallelism;
    }

    @Nullable
    @Override
    public Permission getRequiredPermission() {
        return requiredPermission;
    }

    /**
     * If true, only one instance of given supplier will be created.
     */
    public DbCheckingPMetaSupplier forceTotalParallelismOne(boolean forceTotalParallelismOne) {
        this.forceTotalParallelismOne = forceTotalParallelismOne;
        return this;
    }

    @Override
    public boolean initIsCooperative() {
        return !shouldCheck;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        if (forceTotalParallelismOne) {
            preferredLocalParallelism = 1;
            if (context.localParallelism() != 1) {
                throw new IllegalArgumentException(
                        "Local parallelism of " + context.localParallelism() + " was requested for a vertex that "
                                + "supports only total parallelism of 1. Local parallelism must be 1.");
            }
            String key = getPartitionKey(newUnsecureUuidString());
            int partitionId = context.hazelcastInstance().getPartitionService().getPartition(key).getPartitionId();
            ownerAddress = context.partitionAssignment().entrySet().stream()
                                  .filter(en -> arrayIndexOf(partitionId, en.getValue()) >= 0)
                                  .findAny()
                                  .map(Entry::getKey)
                                  .orElseThrow(() -> new RuntimeException("Owner partition not assigned to any " +
                                          "participating member"));
        }

        if (shouldCheck) {
            Tuple2<MongoClient, DataConnection> clientAndRef = connect(context);
            MongoClient client = clientAndRef.requiredF0();
            try {
                if (databaseName != null) {
                    checkDatabaseExists(client, databaseName);
                    MongoDatabase database = client.getDatabase(databaseName);
                    if (collectionName != null) {
                        checkCollectionExists(database, collectionName);
                    }
                }
            } finally {
                DataConnection connection = clientAndRef.f1();
                if (connection != null) {
                    connection.release();
                }
            }
        }
    }

    private Tuple2<MongoClient, DataConnection> connect(Context context) {
        try {
            if (clientSupplier != null) {
                return tuple2(clientSupplier.get(), null);
            } else if (dataConnectionRef != null) {
                NodeEngine nodeEngine = Util.getNodeEngine(context.hazelcastInstance());
                DataConnectionService dataConnectionService = nodeEngine.getDataConnectionService();
                var dataConnection = dataConnectionService.getAndRetainDataConnection(dataConnectionRef.getName(),
                        MongoDataConnection.class);
                return tuple2(dataConnection.getClient(), dataConnection);
            } else {
                throw new IllegalArgumentException("Either connectionSupplier or dataConnectionRef must be provided " +
                        "if database and collection existence checks are requested");
            }
        } catch (Exception e) {
            throw new JetException("Cannot connect to MongoDB", e);
        }
    }

    @Nonnull
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        if (forceTotalParallelismOne) {
            return addr -> addr.equals(ownerAddress) ? processorSupplier : count -> singletonList(new ExpectNothingP());
        } else {
            return addr -> processorSupplier;
        }
    }

    @Override
    public boolean closeIsCooperative() {
        return true;
    }

}
