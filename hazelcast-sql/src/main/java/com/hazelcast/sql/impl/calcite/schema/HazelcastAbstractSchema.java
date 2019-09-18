/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.schema;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.calcite.HazelcastCalciteContext;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for all schemas.
 */
public class HazelcastAbstractSchema extends AbstractSchema {
    /** Node engine. */
    protected final NodeEngine nodeEngine;

    public HazelcastAbstractSchema(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public Schema snapshot(SchemaVersion  version) {
        return this;
    }

    /**
     * Get all partitioned tables.
     *
     * @return Partitioned tables.
     */
    protected Map<String, Table> getPartitionedTables() {
        return getTables0(MapService.SERVICE_NAME, HazelcastCalciteContext.Key.PARTITIONED_MAPS);
    }

    /**
     * Get all replicated tables.
     *
     * @return Replicated tables.
     */
    protected Map<String, Table> getReplicatedTables() {
        return getTables0(ReplicatedMapService.SERVICE_NAME, HazelcastCalciteContext.Key.REPLICATED_MAPS);
    }

    private Map<String, Table> getTables0(String serviceName, HazelcastCalciteContext.Key contextKey) {
        HazelcastCalciteContext context = HazelcastCalciteContext.get();

        Map<String, Table> res = context.getData(contextKey);

        if (res != null)
            return res;

        Collection<String> mapNames = nodeEngine.getProxyService().getDistributedObjectNames(serviceName);

        res = new HashMap<>();

        for (String mapName : mapNames) {
            DistributedObject map = nodeEngine.getProxyService().getDistributedObject(serviceName, mapName);

            HazelcastTable table = new HazelcastTable(map);

            res.put(mapName, table);
        }

        context.setData(contextKey, res);

        return res;
    }
}
