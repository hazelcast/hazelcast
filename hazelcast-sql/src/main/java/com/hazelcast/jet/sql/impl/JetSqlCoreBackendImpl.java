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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.map.JetMapMetadataResolverImpl;
import com.hazelcast.jet.sql.impl.schema.MappingCatalog;
import com.hazelcast.jet.sql.impl.schema.MappingStorage;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.JetSqlCoreBackend;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.JetMapMetadataResolver;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class JetSqlCoreBackendImpl implements JetSqlCoreBackend, ManagedService {

    private MappingCatalog catalog;
    private JetSqlBackend sqlBackend;
    private Map<Long, JetQueryResultProducer> resultConsumerRegistry;

    @SuppressWarnings("unused") // used through reflection
    public void init(@Nonnull AbstractJetInstance jetInstance) {
        HazelcastInstanceImpl hazelcastInstance = (HazelcastInstanceImpl) jetInstance.getHazelcastInstance();
        NodeEngine nodeEngine = hazelcastInstance.node.nodeEngine;

        MappingStorage mappingStorage = new MappingStorage(nodeEngine);
        SqlConnectorCache connectorCache = new SqlConnectorCache(nodeEngine);
        MappingCatalog mappingCatalog = new MappingCatalog(nodeEngine, mappingStorage, connectorCache);

        this.resultConsumerRegistry = new ConcurrentHashMap<>();
        JetPlanExecutor planExecutor = new JetPlanExecutor(mappingCatalog, jetInstance, resultConsumerRegistry);
        this.catalog = mappingCatalog;
        this.sqlBackend = new JetSqlBackend(nodeEngine, planExecutor);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public List<TableResolver> tableResolvers() {
        return Collections.singletonList(catalog);
    }

    @Override
    public JetMapMetadataResolver mapMetadataResolver() {
        return JetMapMetadataResolverImpl.INSTANCE;
    }

    @Override
    public Object sqlBackend() {
        return sqlBackend;
    }

    @Override
    public SqlResult execute(QueryId queryId, SqlPlan plan, List<Object> arguments, long timeout, int pageSize) {
        if (timeout > 0) {
            throw new JetException("Query timeout not yet supported");
        }
        // TODO: query page size defaults to 4096

        return ((JetPlan) plan).execute(queryId, arguments);
    }

    public Map<Long, JetQueryResultProducer> getResultConsumerRegistry() {
        return resultConsumerRegistry;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }
}
