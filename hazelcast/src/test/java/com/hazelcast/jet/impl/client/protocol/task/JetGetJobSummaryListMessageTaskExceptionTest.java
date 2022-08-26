package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.cluster.Member;
import com.hazelcast.instance.SimpleMemberImpl;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetGetJobSummaryListMessageTaskExceptionTest extends AbstractJetMultiTargetMessageTaskTest {
    @Test
    public void when_reducingWithMemberLeftException_then_exceptionIsNotRethrown() throws Throwable {
        JetGetJobSummaryListMessageTask task = new JetGetJobSummaryListMessageTask(null, node, connection);
        assertEquals(0, ((Collection<?>) task.reduce(MEMBER_LEFT_EXCEPTION_RESULT)).size());
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

        JobSummary summary = new JobSummary(true, 0, 0, "", JobStatus.RUNNING, 0, 0, "");
        List<JobSummary> summaryList = Collections.singletonList(summary);
        Map<Member, Object> singleResult = Collections.singletonMap(new SimpleMemberImpl(), summaryList);

        List<?> result = (List<?>) task.reduce(singleResult);
        assertEquals(1, result.size());
        assertEquals(summary, result.get(0));
    }

    @Test
    public void when_reducingWithDuplicatedEntries_then_singleEntryIsReturned() throws Throwable {
        JetGetJobSummaryListMessageTask task = new JetGetJobSummaryListMessageTask(null, node, connection);

        JobSummary summary = new JobSummary(true, 0, 0, "", JobStatus.RUNNING, 0, 0, "");
        List<JobSummary> summaryList = Collections.singletonList(summary);
        Map<Member, Object> duplicatedResults = new HashMap<>();
        duplicatedResults.put(new SimpleMemberImpl(), summaryList);
        duplicatedResults.put(new SimpleMemberImpl(), summaryList);

        assertEquals(2, duplicatedResults.size());

        List<?> result = (List<?>) task.reduce(duplicatedResults);
        assertEquals(1, result.size());
        assertEquals(summary, result.get(0));
    }
}
