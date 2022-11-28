package com.hazelcast.jet.sql.impl.s2sjoin;

import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.test.HazelcastParametrizedRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
public class SqlSTSJoinFaultToleranceStressTest extends SqlTestSupport {

    @Parameter
    public ProcessingGuarantee processingGuarantee;

    @Parameters(name = "{0}")
    public static Iterable<Object> parameters() {
        return asList(NONE, AT_LEAST_ONCE, EXACTLY_ONCE);
    }

    @Test
    public void stressTest() {

    }
}
