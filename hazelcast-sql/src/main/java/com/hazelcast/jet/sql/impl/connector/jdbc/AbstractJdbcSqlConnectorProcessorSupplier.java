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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.datastore.ExternalDataStoreFactory;
import com.hazelcast.datastore.ExternalDataStoreService;
import com.hazelcast.datastore.impl.CloseableDataSource;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import static java.util.Objects.requireNonNull;

abstract class AbstractJdbcSqlConnectorProcessorSupplier implements ProcessorSupplier {

    protected String externalDataStoreRef;

    protected transient DataSource dataSource;

    AbstractJdbcSqlConnectorProcessorSupplier() {
    }

    AbstractJdbcSqlConnectorProcessorSupplier(String externalDataStoreRef) {
        this.externalDataStoreRef = requireNonNull(externalDataStoreRef, "externalDataStoreRef must not be null");
    }

    public void init(@Nonnull Context context) throws Exception {
        ExternalDataStoreService externalDataStoreService = ((HazelcastInstanceImpl) context.hazelcastInstance())
                .node.getNodeEngine().getExternalDataStoreService();

        ExternalDataStoreFactory<DataSource> factory = externalDataStoreService
                .getExternalDataStoreFactory(externalDataStoreRef);

        dataSource = factory.getDataStore();
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {
        if (dataSource instanceof CloseableDataSource) {
            ((CloseableDataSource) dataSource).close();
        }
    }
}
