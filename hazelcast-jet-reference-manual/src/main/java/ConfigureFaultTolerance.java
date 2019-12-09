/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;

import java.io.File;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ConfigureFaultTolerance {

    static void s1() {
        //tag::s1[]
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jobConfig.setSnapshotIntervalMillis(SECONDS.toMillis(10));
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setBackupCount(2);
        JetInstance instance = Jet.newJetInstance(config);
        //end::s2[]
    }

    static void s3() {
        JobConfig jobConfig = null;
        //tag::s3[]
        jobConfig.setSplitBrainProtection(true);
        //end::s3[]
    }

    static void s4() {
        //tag::s4[]
        JetConfig cfg = new JetConfig();
        cfg.getInstanceConfig().setLosslessRestartEnabled(true);
        cfg.getHazelcastConfig().getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(new File("/mnt/hot-restart"))
                .setParallelism(2);
        //end::s4[]
    }

    static void s5() {
        JetInstance jet = null;
        //tag::s5[]
        jet.getCluster().shutdown();
        //end::s5[]
    }

    static void s6() {
        //tag::s6[]
        JetConfig config = new JetConfig();
        config.getProperties().setProperty("hazelcast.rest.enabled", "true");
        //end::s6[]
    }

    static void s7() {
        //tag::s7[]
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("clusterA");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress("10.216.1.18", "10.216.1.19");
        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.setClusterName("clusterB");
        ClientNetworkConfig networkConfig2 = clientConfig2.getNetworkConfig();
        networkConfig2.addAddress( "10.214.2.10", "10.214.2.11");
        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        clientFailoverConfig.addClientConfig(clientConfig).addClientConfig(clientConfig2).setTryCount(10);
        JetInstance client = Jet.newJetFailoverClient(clientFailoverConfig);
        //end::s7[]
    }

    static void s8() {
        //tag::s8[]
        //end::s8[]
    }

    static void s9() {
        //tag::s9[]
        //end::s9[]
    }

    static void s10() {
        //tag::s10[]
        //end::s10[]
    }

    static void s11() {
        //tag::s11[]
        //end::s11[]
    }
}
