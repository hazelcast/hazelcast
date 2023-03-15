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

package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.cluster.Member;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetGetJobSummaryListMessageTaskExceptionTest extends AbstractJetMultiTargetMessageTaskTest {
    private static final JobSummary SUMMARY = new JobSummary(true, 0, 0, "", JobStatus.RUNNING, 0, 0, "");

    @Test
    public void when_reducingWithIgnoredExceptions_then_exceptionIsNotRethrown() throws Throwable {
        JetGetJobSummaryListMessageTask task = new JetGetJobSummaryListMessageTask(null, node, connection);
        assertEquals(0, ((Collection<?>) task.reduce(IGNORED_EXCEPTIONS_RESULT)).size());
    }

    @Test
    public void when_reducingWithException_then_exceptionIsRethrown() {
        JetGetJobSummaryListMessageTask task = new JetGetJobSummaryListMessageTask(null, node, connection);

        assertThatExceptionOfType(OtherException.class)
                .isThrownBy(() -> {
                    task.reduce(OTHER_EXCEPTION_RESULT);
                })
                .withMessage(OTHER_EXCEPTION_MESSAGE);
    }

    @Test
    public void when_reducingWithSingleEntry_then_entryIsReturned() throws Throwable {
        JetGetJobSummaryListMessageTask task = new JetGetJobSummaryListMessageTask(null, node, connection);

        Map<Member, Object> singleResult = prepareSingleResultMap(SUMMARY);

        List<?> result = (List<?>) task.reduce(singleResult);
        assertEquals(1, result.size());
        assertEquals(SUMMARY, result.get(0));
    }

    @Test
    public void when_reducingWithDuplicatedEntries_then_singleEntryIsReturned() throws Throwable {
        JetGetJobSummaryListMessageTask task = new JetGetJobSummaryListMessageTask(null, node, connection);

        Map<Member, Object> duplicatedResults = prepareDuplicatedResult(SUMMARY);

        assertEquals(2, duplicatedResults.size());

        List<?> result = (List<?>) task.reduce(duplicatedResults);
        assertEquals(1, result.size());
        assertEquals(SUMMARY, result.get(0));
    }
}
