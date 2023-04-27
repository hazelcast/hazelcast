/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlWithoutSqlModuleTest extends JetTestSupport {

    @Test
    public void test() {
        HazelcastInstance inst = createHazelcastInstance();
        assertThatThrownBy(() -> inst.getSql().execute("SELECT 1"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("Cannot execute SQL query because \"hazelcast-sql\" module is not on the classpath");
    }

    @Test
    public void clientTest() {
        HazelcastInstance inst = createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();

        try {
            client.getSql().execute("SELECT 1");
            fail("should have failed");
        } catch (HazelcastSqlException e) {
            assertNotNull(e.getOriginatingMemberId());
            assertEquals(Util.getNodeEngine(inst).getNode().getThisUuid(), e.getOriginatingMemberId());
        }
    }
}
