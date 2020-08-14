/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.plan.Plan;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class PlanCacheTestSupport extends SqlTestSupport {

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
    public static PlanCacheKey createKey(String sql) {
        return new PlanCacheKey(Collections.emptyList(), sql);
    }

    public static Plan createPlan(PlanCacheKey key, Map<UUID, PartitionIdSet> partMap, int... objectIds) {
        Set<PlanObjectKey> objectIds0 = new HashSet<>();

        if (objectIds != null) {
            for (int objectId : objectIds) {
                objectIds0.add(createObjectId(objectId));
            }
        }

        Plan plan = new Plan(
            partMap,
            null,
            null,
            null,
            null,
            null,
            null,
            QueryParameterMetadata.EMPTY,
            key,
            objectIds0
        );

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

    public static class TestPlanObjectKey implements PlanObjectKey {

        private final int id;

        public TestPlanObjectKey(int id) {
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
}
