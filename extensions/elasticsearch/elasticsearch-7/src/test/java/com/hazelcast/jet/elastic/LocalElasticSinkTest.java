/*
 * Copyright 2020 Hazelcast Inc.
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

package com.hazelcast.jet.elastic;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import org.junit.After;

/**
 * Test running single Jet member locally and Elastic in docker
 */
public class LocalElasticSinkTest extends CommonElasticSinksTest {

    private JetTestInstanceFactory factory = new JetTestInstanceFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Override
    protected JetInstance createJetInstance() {
        // This starts very quickly, no need to cache the instance
        return factory.newMember(new JetConfig());
    }

}
