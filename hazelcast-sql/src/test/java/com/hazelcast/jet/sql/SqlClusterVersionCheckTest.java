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

package com.hazelcast.jet.sql;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.version.Version;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlClusterVersionCheckTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);

        initializeWithClient(1, config, new ClientConfig());
    }

    @Test
    public void when_clusterVersionIsLessThan5_0_exceptionIsThrown() {
        setClusterVersion(Versions.V4_2);

        Assert.assertThrows("SQL queries cannot be executed on clusters with version less than 5.0",
                HazelcastSqlException.class,
                () -> client().getSql().execute("SELECT 1").updateCount());
    }

    @Test
    public void when_clusterVersionIsEqualTo5_0_queryIsExecuted() {
        setClusterVersion(Versions.V5_0);
        Assert.assertEquals(1, client().getSql().execute("SELECT 1").getRowMetadata().getColumnCount());
    }

    @Test
    public void when_clusterVersionIsGreaterThan5_0_queryIsExecuted() {
        setClusterVersion(Version.of(5, 1));
        Assert.assertEquals(1, client().getSql().execute("SELECT 1").getRowMetadata().getColumnCount());
    }

    private void setClusterVersion(Version version) {
        ((ClusterServiceImpl) ((HazelcastInstanceProxy) instance())
                .getOriginal()
                .node
                .getNodeEngine()
                .getClusterService())
                .getClusterStateManager()
                .setClusterVersion(version);
    }
}
