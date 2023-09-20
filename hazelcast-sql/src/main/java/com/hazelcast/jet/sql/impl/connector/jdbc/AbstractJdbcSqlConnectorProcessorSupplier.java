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

import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.DataSourceFromConnectionSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import static java.util.Objects.requireNonNull;

abstract class AbstractJdbcSqlConnectorProcessorSupplier implements ProcessorSupplier {

    protected String dataConnectionName;

    protected transient JdbcDataConnection dataConnection;
    protected transient DataSource dataSource;

    AbstractJdbcSqlConnectorProcessorSupplier() {
    }

    AbstractJdbcSqlConnectorProcessorSupplier(String dataConnectionName) {
        this.dataConnectionName = requireNonNull(dataConnectionName, "dataConnectionName must not be null");
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        dataConnection = context.dataConnectionService()
                                .getAndRetainDataConnection(dataConnectionName, JdbcDataConnection.class);
        dataSource = new DataSourceFromConnectionSupplier(dataConnection::getConnection);
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {
        if (dataConnection != null) {
            dataConnection.release();
        }
    }
}
