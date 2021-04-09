/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URL;
import java.net.URLClassLoader;

@Ignore
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDeployment_StandaloneClusterTest extends JetTestSupport {

    @Test
    public void when_classAddedUsingUcd_then_visibleToJet() throws Exception {
        URL classUrl = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> personClz = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");

        ClientConfig jetClientConfig = new ClientConfig();
        jetClientConfig.setClassLoader(urlClassLoader);
        jetClientConfig.getUserCodeDeploymentConfig()
                       .setEnabled(true)
                       .addClass(personClz);

        Config config = new Config();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        JetInstance instance = createJetMember(config);
        JetInstance client = createJetClient(jetClientConfig);

        DAG dag = new DAG();
        dag.newVertex("v", () -> new LoadClassesIsolated(true));

        instance.newJob(dag).join();
    }
}
