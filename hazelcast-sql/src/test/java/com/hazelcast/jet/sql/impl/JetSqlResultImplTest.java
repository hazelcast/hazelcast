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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class JetSqlResultImplTest extends JetTestSupport {

    @Test
    // test for https://github.com/hazelcast/hazelcast-jet/issues/2697
    public void when_closed_then_iteratorFails() {
        SqlResult sqlResult = createHazelcastInstance().getSql().execute("select * from table(generate_stream(1))");
        sqlResult.close();
        Iterator<SqlRow> iterator = sqlResult.iterator();
        assertThatThrownBy(() -> iterator.forEachRemaining(ConsumerEx.noop()));
    }
}
