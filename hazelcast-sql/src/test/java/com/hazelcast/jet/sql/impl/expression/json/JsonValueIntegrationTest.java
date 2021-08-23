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

package com.hazelcast.jet.sql.impl.expression.json;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonValueIntegrationTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void when_calledWithBasicSyntax_objectValueIsReturned() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue("[1,2,3]"));

        for (final SqlRow row : instance().getSql().execute("SELECT JSON_VALUE(this, '$[1]') FROM test")) {
            System.out.println(row);
        }
    }

    @Test
    public void when_calledWithReturning_correctTypeIsReturned() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue("[1,2,3]"));

        for (final SqlRow row : instance().getSql().execute("SELECT * FROM test WHERE JSON_VALUE(this, '$[1]' RETURNING BIGINT) > 1 ")) {
            System.out.println(row);
        }
    }
}
