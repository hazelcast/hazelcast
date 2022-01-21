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

package com.hazelcast.jet.sql.impl.optimizer;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import org.junit.Test;

import static java.util.Collections.singletonList;

public class PlanKeyTest extends SqlTestSupport {

    @Test
    public void testEquals() {
        PlanKey key = new PlanKey(singletonList(singletonList("schema1")), "sql1");

        checkEquals(key, new PlanKey(singletonList(singletonList("schema1")), "sql1"), true);

        checkEquals(key, new PlanKey(singletonList(singletonList("schema2")), "sql1"), false);
        checkEquals(key, new PlanKey(singletonList(singletonList("schema1")), "sql2"), false);
    }
}
