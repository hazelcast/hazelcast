/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.FilteringClassLoader;

import java.lang.reflect.Method;
import java.util.Collections;
import org.junit.After;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
@Ignore
public class DeploymentTest extends AbstractDeploymentTest {

    private JetInstance instance;
    private Object isolatedNode;


    @After
    public void tearDown() {
        Jet.shutdownAll();
        shutdownIsolatedNode();
    }

    @Override
    protected JetInstance getJetInstance() {
        return instance;
    }

    @Override
    protected void createCluster() {
        JetConfig jetConfig = new JetConfig();
        instance = Jet.newJetInstance(jetConfig);
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            isolatedNode = createIsolatedNode(thread,
                    new FilteringClassLoader(Collections.singletonList("deployment"), "com.hazelcast"));
        } catch (Exception e) {
            throw new RuntimeException("Could not start isolated Hazelcast instance", e);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    private void shutdownIsolatedNode() {
        if (isolatedNode == null) {
            return;
        }
        try {
            Class<?> instanceClass = isolatedNode.getClass();
            Method method = instanceClass.getMethod("shutdown");
            method.invoke(isolatedNode);
            isolatedNode = null;
        } catch (Exception e) {
            throw new RuntimeException("Could not start shutdown Hazelcast instance", e);
        }
    }


}
