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

package com.hazelcast.sql;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.map.IMap;
import org.junit.Test;

import java.io.Serializable;

public class StaleIndexTest extends JetTestSupport {
    private HazelcastInstance inst;
    private IMap<Integer, FooTuple> map;

    @Test
    public void test() {
        inst = createHazelcastInstance();
        map = inst.getMap("m");

        createIndexAndExecuteQuery("f0");
//        map.destroy();
        createIndexAndExecuteQuery("f1");

        sleepSeconds(2); // plan should be invalidated now
        createIndexAndExecuteQuery("f1");
    }

    private void createIndexAndExecuteQuery(String indexedField) {
        map.addIndex(new IndexConfig(IndexType.SORTED, indexedField).setName("foo"));
        map.put(0, new FooTuple("v0", "v1"));
        map.put(1, new FooTuple("v1", "v0"));

        for (SqlRow row : inst.getSql().execute("select __key from m where f0='v0'")) {
            System.out.println(row);
        }
        System.out.println("done");
    }

    public static class FooTuple implements Serializable {
        public String f0;
        public String f1;

        public FooTuple(String f0, String f1) {
            this.f0 = f0;
            this.f1 = f1;
        }
    }
}
