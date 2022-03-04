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
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlResultImplTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    // test for https://github.com/hazelcast/hazelcast-jet/issues/2697
    public void when_closed_then_iteratorFails() {
        SqlResult sqlResult = instance().getSql().execute("select * from table(generate_stream(1))");
        sqlResult.close();
        Iterator<SqlRow> iterator = sqlResult.iterator();
        assertThatThrownBy(() -> iterator.forEachRemaining(ConsumerEx.noop()));
    }

    @Test
    public void when_hasNextInterrupted_then_interrupted() {
        // this query is a continuous one, but never returns any rows (all are filtered out)
        SqlResult sqlResult = instance().getSql().execute("select * from table(generate_stream(1)) where v < 0");
        AtomicBoolean interruptedOk = new AtomicBoolean();
        Thread t = new Thread(() -> {
            try {
                sqlResult.iterator().hasNext();
            } catch (Throwable e) {
                if (e.getCause() instanceof RuntimeException
                        && e.getCause().getCause() instanceof InterruptedException) {
                    interruptedOk.set(true);
                } else {
                    logger.severe("Unexpected exception caught", e);
                }
            }
        });
        t.start();
        t.interrupt();
        assertTrueEventually(() -> assertTrue(interruptedOk.get()));
    }
}
