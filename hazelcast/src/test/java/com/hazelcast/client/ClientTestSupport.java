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

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * @author mdogan 5/14/13
 */
@RunWith(HazelcastSerialClassRunner.class)
public abstract class ClientTestSupport extends HazelcastTestSupport {

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
            client = newClient(TestUtil.getNode(instance));
            client.auth();
        }

        protected void after() {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            instance.shutdown();
        }
    }

    public static SimpleClient newClient(Node node) throws IOException {
        if (node.isActive()) {
            if (TestEnvironment.isMockNetwork()) {
                ClientEngineImpl engine = node.clientEngine;
                return new MockSimpleClient(engine, node.getSerializationService());
            } else {
                return new SocketSimpleClient(node);
            }
        }
        throw new IllegalArgumentException("Node is not active: " + node.getThisAddress());
    }
}
