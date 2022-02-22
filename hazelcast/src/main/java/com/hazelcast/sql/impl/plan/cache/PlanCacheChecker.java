/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.plan.cache;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.optimizer.PlanCheckContext;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Checks plans for validity.
 */
public class PlanCacheChecker {

    private final NodeEngine nodeEngine;
    private final PlanCache planCache;
    private final List<TableResolver> tableResolvers;

    public PlanCacheChecker(NodeEngine nodeEngine, PlanCache planCache, List<TableResolver> tableResolvers) {
        this.nodeEngine = nodeEngine;
        this.planCache = planCache;
        this.tableResolvers = tableResolvers;
        this.tableResolvers.forEach(tableResolver -> tableResolver.registerListener(this::check));
    }

    public void check() {
        if (planCache.size() == 0) {
            return;
        }

        // Collect object IDs
        SqlCatalog catalog = new SqlCatalog(tableResolvers);

        Set<PlanObjectKey> objectKeys = new HashSet<>();

        for (Map<String, Table> tableMap : catalog.getSchemas().values()) {
            for (Table table : tableMap.values()) {
                PlanObjectKey objectKey = table.getObjectKey();

                if (objectKey != null) {
                    objectKeys.add(objectKey);
                }
            }
        }

        // Prepare partition distribution
        Map<UUID, PartitionIdSet> partitions = QueryUtils.createPartitionMap(nodeEngine, null, false);

        // Do check
        planCache.check(new PlanCheckContext(objectKeys, partitions));
    }
}
