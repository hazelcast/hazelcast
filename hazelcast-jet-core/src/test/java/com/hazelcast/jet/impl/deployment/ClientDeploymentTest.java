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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;

import static java.util.Collections.singletonList;

@RunWith(HazelcastSerialClassRunner.class)
public class ClientDeploymentTest extends AbstractDeploymentTest {

    @Rule
    public final Timeout timeoutRule = Timeout.seconds(360);

    private Object isolatedNode;
    private JetInstance client;

    @After
    public void tearDown() {
        Jet.shutdownAll();
        shutdownIsolatedNode();
    }


    @Override
    protected JetInstance getJetInstance() {
        return client;
    }

    @Override
    protected void createCluster() {
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        String host;
        Integer port;
        try {
            FilteringClassLoader cl = new FilteringClassLoader(singletonList("deployment"), "com.hazelcast");
            isolatedNode = createIsolatedNode(thread, cl);
            Class<?> jetInstanceClazz = cl.loadClass("com.hazelcast.jet.JetInstance");
            Method getCluster = jetInstanceClazz.getDeclaredMethod("getCluster");
            Object clusterObj = getCluster.invoke(isolatedNode);
            Class<?> cluster = cl.loadClass("com.hazelcast.core.Cluster");
            Method getLocalMember = cluster.getDeclaredMethod("getLocalMember");
            Object memberObj = getLocalMember.invoke(clusterObj);
            Class<?> member = cl.loadClass("com.hazelcast.core.Member");
            Method getAddress = member.getDeclaredMethod("getAddress");
            Object addressObj = getAddress.invoke(memberObj);
            Class<?> address = cl.loadClass("com.hazelcast.nio.Address");
            Method getHost = address.getDeclaredMethod("getHost");
            Method getPort = address.getDeclaredMethod("getPort");
            host = (String) getHost.invoke(addressObj);
            port = (Integer) getPort.invoke(addressObj);
        } catch (Exception e) {
            throw new RuntimeException("Could not start isolated Hazelcast instance", e);
        } finally {
            thread.setContextClassLoader(tccl);
        }
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("jet");
        clientConfig.getGroupConfig().setPassword("jet-pass");
        clientConfig.getNetworkConfig().setAddresses(singletonList(host + ":" + port));
        client = Jet.newJetClient(clientConfig);
    }

    protected void shutdownIsolatedNode() {
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
