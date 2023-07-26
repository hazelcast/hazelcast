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

package com.hazelcast.sql.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.impl.DataConnectionServiceImpl;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;

@NotThreadSafe
public class DataConnectionConsistencyChecker {
    private final HazelcastInstance hazelcastInstance;
    private final NodeEngine nodeEngine;
    private final DataConnectionServiceImpl dataConnectionService;
    private final ILogger logger;
    // sqlCatalog supposed to be initialized lazily
    private IMap<Object, Object> sqlCatalog;

    private boolean initialized;

    public DataConnectionConsistencyChecker(HazelcastInstance instance, NodeEngine nodeEngine) {
        this.hazelcastInstance = instance;
        this.nodeEngine = nodeEngine;
        this.dataConnectionService = (DataConnectionServiceImpl) nodeEngine.getDataConnectionService();
        this.logger = nodeEngine.getLogger(DataConnectionConsistencyChecker.class);
    }

    /**
     * Lazily initialize sqlCatalog.
     * The only reason of this method existing is
     * to prevent eager creation of SQL catalog IMap.
     */
    public void init() {
        sqlCatalog = hazelcastInstance.getMap(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        initialized = true;
    }

    /**
     * Removes & alters all outdated data connections and adds
     * all missed data connections to data connection service
     */
    public void check() {
        if (!initialized) {
            return;
        }

        if (!nodeEngine.getPartitionService().isPartitionAssignmentDone()
            && !hazelcastInstance.getCluster().getClusterState().isMigrationAllowed()) {
            // partitions have not been assigned yet and migrations are not allowed
            // so sql catalog can have no values -> skip check, because we anyway cannot
            // iterate over sqlCatalog values (operation will fail with exception, since
            // partition assignments do not exist and cannot be triggered).
            logger.info("Skipping data connection consistency check because"
                    + " initial partition assignment is not done yet.");
            return;
        }

        // capture data connections set before altering it.
        List<DataConnection> sqlDataConnections = dataConnectionService.getSqlCreatedDataConnections();

        for (Object catalogItem : sqlCatalog.values()) {
            if (!(catalogItem instanceof DataConnectionCatalogEntry)) {
                continue;
            }
            DataConnectionCatalogEntry dl = (DataConnectionCatalogEntry) catalogItem;

            // If a data connection is found in the catalog that conflicts in name with one
            // in the config, the one from the catalog should be deleted.
            if (dataConnectionService.existsConfigDataConnection(dl.name())) {
                sqlCatalog.remove(QueryUtils.wrapDataConnectionKey(dl.name()));
                continue;
            }

            dataConnectionService.createOrReplaceSqlDataConnection(dl.name(), dl.type(), dl.isShared(), dl.options());
        }

        for (DataConnection dataConnection : sqlDataConnections) {
            Object catalogValue = sqlCatalog.get(QueryUtils.wrapDataConnectionKey(dataConnection.getName()));

            // Data connection is absent in sql catalog -> remove it from data connection service.
            if (catalogValue == null) {
                dataConnectionService.removeDataConnection(dataConnection.getName());
            }
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

}
