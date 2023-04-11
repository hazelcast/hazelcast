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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.impl.util.Util;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;

import javax.annotation.Nonnull;
import java.util.Collections;

import static com.hazelcast.internal.nio.IOUtil.closeResource;

/**
 * Processor for querying MongoDB
 * <p>
 *  This is a one-off, batch processor that will given {@link WriteModel} on all items matching
 *  given predicate.
 *
 * @param <I> type of saved item
 */
public class UpdateMongoP<I> extends AbstractProcessor {

    private final MongoConnection connection;
    private final String collectionName;
    private final String databaseName;

    private final SupplierEx<WriteModel<I>> writeModelFunction;
    private final Class<I> documentType;
    /**
     * Creates a new processor that will always insert to the same database and collection.
     */
    public UpdateMongoP(WriteMongoParams<I> params,
                        SupplierEx<WriteModel<I>> writeModelFunction
    ) {
        this.connection = new MongoConnection(params.clientSupplier, params.dataConnectionRef, client -> {
        });

        this.writeModelFunction = writeModelFunction;
        this.documentType = params.documentType;
        collectionName = params.collectionName;
        databaseName = params.databaseName;
    }

    @Override
    protected void init(@Nonnull Context context) {
        connection.assembleSupplier(Util.getNodeEngine(context.hazelcastInstance()));
    }

    @Override
    public void close() {
        closeResource(connection);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        if (!connection.reconnectIfNecessary()) {
            return false;
        }

        try {
            MongoCollection<I> collection = connection.client()
                                                      .getDatabase(databaseName)
                                                      .getCollection(collectionName, documentType);

            WriteModel<I> writeModel = writeModelFunction.get();
            collection.bulkWrite(Collections.singletonList(writeModel));
        } catch (Exception e) {
            throw new JetException("Unable to perform query", e);
        }
        return true;
    }
}
