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

package com.hazelcast.sql.impl.client;

import com.hazelcast.client.config.ClientSqlResubmissionMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.sql.SqlTestSupport;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public abstract class SqlResubmissionTestSupport extends SqlTestSupport {
    protected static final String FAILING_MAP_NAME = randomName();
    protected static final int COMMON_MAP_SIZE = 10_000;
    protected static final String COMMON_MAP_NAME = randomName();

    protected boolean shouldFailAfterSomeDataIsFetched(ClientSqlResubmissionMode mode) {
        return mode == ClientSqlResubmissionMode.NEVER || mode == ClientSqlResubmissionMode.RETRY_SELECTS;
    }

    protected boolean shouldFailBeforeAnyDataIsFetched(ClientSqlResubmissionMode mode) {
        return mode == ClientSqlResubmissionMode.NEVER;
    }

    /** Return true, if, in the given mode, the command should fail, if it's not a SELECT query. */
    protected boolean shouldFailNonSelectQuery(ClientSqlResubmissionMode mode) {
        return mode != ClientSqlResubmissionMode.RETRY_ALL;
    }

    protected <T> void createMap(
            HazelcastInstance instance,
            String name,
            int size,
            Supplier<T> objectCreator,
            Class<T> tClass
    ) {
        Map<Integer, T> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            map.put(i, objectCreator.get());
        }
        instance.getMap(name).putAll(map);
        createMapping(instance, name, Integer.class, tClass);
    }

    public static class FailingDuringFieldAccessObject implements Serializable {
        private int field = 0;

        public int getField() {
            throw new TopologyChangedException();
        }

        public void setField(int field) {
            this.field = field;
        }
    }

    public static class IntHolder implements Serializable {
        private int field = 0;

        public IntHolder() {
        }

        public IntHolder(int field) {
            this.field = field;
        }

        public int getField() {
            return field;
        }

        public void setField(int field) {
            this.field = field;
        }
    }
}
