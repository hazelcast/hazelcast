/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.cache;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.optimizer.PlanCheckContext;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public abstract class PlanCacheTestSupport extends SqlTestSupport {

    protected static final Map<UUID, PartitionIdSet> PART_MAP_1;
    protected static final Map<UUID, PartitionIdSet> PART_MAP_2;

    static {
        UUID memberId = UuidUtil.newSecureUUID();

        PartitionIdSet partitions1 = new PartitionIdSet(4);
        partitions1.add(0);
        partitions1.add(1);

        PartitionIdSet partitions2 = new PartitionIdSet(4);
        partitions2.add(2);
        partitions2.add(3);

        PART_MAP_1 = Collections.singletonMap(memberId, partitions1);
        PART_MAP_2 = Collections.singletonMap(memberId, partitions2);
    }

    public static PlanKey createKey(String sql) {
        return new PlanKey(Collections.emptyList(), sql);
    }

    public static SqlPlan createPlan(PlanKey key, Map<UUID, PartitionIdSet> partitions, int... objectKeys) {
        Set<PlanObjectKey> objectKeys0 = new HashSet<>();

        if (objectKeys != null) {
            for (int objectId : objectKeys) {
                objectKeys0.add(createObjectId(objectId));
            }
        }
        SqlPlan plan = new TestPlan(key, objectKeys0, partitions);

        assertEquals(key, plan.getPlanKey());
        assertEquals(0L, plan.getPlanLastUsed());

        return plan;
    }

    public static PlanObjectKey createObjectId(int value) {
        return new TestPlanObjectKey(value);
    }

    @SuppressWarnings("BusyWait")
    public static void advanceTime() {
        long currentTime = System.currentTimeMillis();

        try {
            do {
                Thread.sleep(10);

            } while (System.currentTimeMillis() <= currentTime);
        } catch (Exception e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException("Interrupted", e);
        }
    }

    private static class TestPlanObjectKey implements PlanObjectKey {

        private final int id;

        private TestPlanObjectKey(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestPlanObjectKey that = (TestPlanObjectKey) o;

            return id == that.id;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    private static class TestPlan extends SqlPlan {

        private final Set<PlanObjectKey> objectKeys;
        private final Map<UUID, PartitionIdSet> partitions;

        private TestPlan(PlanKey planKey, Set<PlanObjectKey> objectKeys, Map<UUID, PartitionIdSet> partitions) {
            super(planKey);
            this.objectKeys = objectKeys;
            this.partitions = partitions;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return context.isValid(objectKeys, partitions);
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return true;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            throw new UnsupportedOperationException();
        }
    }
}
