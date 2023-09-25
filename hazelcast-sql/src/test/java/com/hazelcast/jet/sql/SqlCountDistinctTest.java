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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlCountDistinctTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Test
    public void test_countDistinct() {
        String mapName = generateRandomString(16);
        createMapping(mapName, Integer.class, Person.class);

        IMap<Object, Object> map = instance().getMap(mapName);

        for (int i = 0; i < 900; i++) {
            String key = "key" + i;
            map.put(key, new Person(i, key));
        }
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            map.put(key + "_", new Person(i, null));
        }

        assertRowsAnyOrder("select count(*) from (select distinct id from " + mapName + ")", rows(1, 900L));
        assertRowsAnyOrder("select count(id) from " + mapName, rows(1, 1000L));
        assertRowsAnyOrder("select count(distinct id) from " + mapName, rows(1, 900L));
        assertRowsAnyOrder("select count(distinct name) from " + mapName, rows(1, 900L));
    }
}
