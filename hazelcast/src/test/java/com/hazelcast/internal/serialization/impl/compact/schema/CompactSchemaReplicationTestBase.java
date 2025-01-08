/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import example.serialization.NodeDTO;
import org.junit.After;
import org.junit.Before;

import java.util.function.IntFunction;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.getSchemasFor;
import static org.mockito.Mockito.spy;

public class CompactSchemaReplicationTestBase extends HazelcastTestSupport {

    private static final String MAP_NAME = "map";

    protected static final Schema SCHEMA = getSchemasFor(NodeDTO.class).iterator().next();

    protected HazelcastInstance[] instances;

    private TestHazelcastInstanceFactory factory;

    protected MemberSchemaService stubbedSchemaService;

    @Before
    public void setUp() {
        factory = new TestHazelcastInstanceFactory();
        stubbedSchemaService = createSpiedMemberSchemaService();
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    public void setupInstances(IntFunction<MemberSchemaService> spyServiceCreator) {
        int instanceCount = 4;
        instances = new HazelcastInstance[instanceCount];
        for (int i = 0; i < 4; i++) {
            MemberSchemaService schemaService = spyServiceCreator.apply(i);
            factory.withNodeExtensionCustomizer(node -> new MemberSchemaServiceMockingNodeExtension(node, schemaService));
            instances[i] = factory.newHazelcastInstance(getConfig());
        }
    }

    @Override
    public Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    protected void fillMapUsing(HazelcastInstance instance) {
        IMap<Integer, NodeDTO> map = instance.getMap(MAP_NAME);
        for (int i = 0; i < 10; i++) {
            map.put(i, new NodeDTO(i));
        }
    }

    protected MemberSchemaService getSchemaService(HazelcastInstance instance) {
        return getNode(instance).getSchemaService();
    }

    protected MemberSchemaService createSpiedMemberSchemaService() {
        return spy(new MemberSchemaService());
    }

    private static class MemberSchemaServiceMockingNodeExtension extends DefaultNodeExtension {
        private final MemberSchemaService schemaService;

        MemberSchemaServiceMockingNodeExtension(Node node, MemberSchemaService schemaService) {
            super(node);
            this.schemaService = schemaService;
        }

        @Override
        public MemberSchemaService createSchemaService() {
            return schemaService;
        }
    }
}
