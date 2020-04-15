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

package com.hazelcast.sql.impl.worker;

import com.hazelcast.sql.impl.LoggingQueryFragmentScheduleCallback;
import com.hazelcast.sql.impl.state.QueryStateCallback;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryFragmentContextTest {
    @Test
    public void testArguments() {
        List<Object> args = new ArrayList<>();

        args.add(new Object());
        args.add(new Object());

        LoggingQueryFragmentScheduleCallback fragmentScheduleCallback = new LoggingQueryFragmentScheduleCallback();
        TestStateCallback stateCallback = new TestStateCallback();

        QueryFragmentContext context = new QueryFragmentContext(args, fragmentScheduleCallback, stateCallback);

        assertSame(args.get(0), context.getArgument(0));
        assertSame(args.get(1), context.getArgument(1));

        context.schedule();
        assertTrue(fragmentScheduleCallback.getCount() > 0);

        context.checkCancelled();
        assertEquals(1, stateCallback.getCheckCancelledInvocationCount());
    }

    private static class TestStateCallback implements QueryStateCallback {

        private int checkCancelledInvocationCount;

        @Override
        public void onFragmentFinished() {
            // No-op.
        }

        @Override
        public void cancel(Exception e) {
            // No-op.
        }

        @Override
        public void checkCancelled() {
            checkCancelledInvocationCount++;
        }

        public int getCheckCancelledInvocationCount() {
            return checkCancelledInvocationCount;
        }
    }
}
