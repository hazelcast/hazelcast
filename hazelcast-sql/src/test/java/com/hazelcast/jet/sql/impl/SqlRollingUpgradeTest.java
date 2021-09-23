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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.version.MemberVersion;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.core.TestProcessors.streamingDag;
import static org.junit.Assert.assertEquals;

public class SqlRollingUpgradeTest extends SqlTestSupport {

    private static HazelcastInstance client;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
        MemberVersion m0Version = instances()[0].getCluster().getLocalMember().getVersion();
        Address m1Address = instances()[1].getCluster().getLocalMember().getAddress();

        // manually set the version for the 2nd member to minor+1
        MemberVersion m1Version = new MemberVersion(m0Version.getMajor(), m0Version.getMinor() + 1, m0Version.getPatch());
        getNodeEngineImpl(instances()[1]).getLocalMember().setVersion(m1Version);
        getNodeEngineImpl(instances()[0]).getClusterService().getMember(m1Address).setVersion(m1Version);

        // create the client after setting the version so that the client knows the correct member versions
        client = factory().newHazelcastClient();
    }

    @Test
    public void when_lightJob_then_usesTheSubmitterVersion() {
        // A light job must be submitted to the local member. Since members have different version, the
        // job will run on the submitting member, but must not run on the other member.
        Job job1 = instances()[0].getJet().newLightJob(streamingDag());
        assertTrueEventually(() -> assertJobExecuting(job1, instances()[0]));
        assertJobNotExecuting(job1, instances()[1]);

        Job job2 = instances()[1].getJet().newLightJob(streamingDag());
        assertTrueEventually(() -> assertJobExecuting(job2, instances()[1]));
        assertJobNotExecuting(job2, instances()[0]);
    }

    @Test
    public void when_clientSql_then_usesEitherVersion() {
        // Since the subsets of members with same version have the same size, the smart client must send the SQL command
        // to the member with newer version.
        createMapping(client, "m", Integer.class, Integer.class);
        client.getSql().execute("select * from table(generate_stream(1)) join m on __key=v");
        assertTrueEventually(() ->
                assertEquals(1, getJetServiceBackend(instances()[1]).getJobExecutionService().getExecutionContexts().size()));
        assertEquals(0, getJetServiceBackend(instances()[0]).getJobExecutionService().getExecutionContexts().size());
    }
}
