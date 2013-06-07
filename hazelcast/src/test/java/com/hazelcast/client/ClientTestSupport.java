/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Set;

/**
 * @mdogan 5/14/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
public abstract class ClientTestSupport {

    @Rule
    public final ClientTestResource clientResource = new ClientTestResource(createConfig());

    protected final HazelcastInstance getInstance() {
        return clientResource.instance;
    }

    protected final SimpleClient getClient() {
        return clientResource.client;
    }

    protected abstract Config createConfig();


    public static final class ClientTestResource extends ExternalResource {
        private final Config config;
        private HazelcastInstance instance;
        private SimpleClient client;

        public ClientTestResource(Config config) {
            this.config = config;
        }

        protected void before() throws Throwable {
            instance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(config);
            final Address address = TestUtil.getNode(instance).getThisAddress();
            client = newClient(address);
            client.auth();
        }

        protected void after() {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            instance.getLifecycleService().shutdown();
        }
    }

    public static SimpleClient newClient(Address nodeAddress) throws IOException {
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        for (HazelcastInstance hz : instances) {
            Node node = TestUtil.getNode(hz);
            MemberImpl m = (MemberImpl) hz.getCluster().getLocalMember();
            if (m.getAddress().equals(nodeAddress)) {
                if (TestEnvironment.isMockNetwork()) {
                    ClientEngineImpl engine = node.clientEngine;
                    return new MockSimpleClient(engine);
                } else {
                    return new SocketSimpleClient(node);
                }
            }
        }
        throw new IllegalArgumentException("Cannot connect to node: " + nodeAddress);
    }
}
