/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;
import static org.mockito.ArgumentMatchers.startsWith;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexCreateTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "map";

    @Parameterized.Parameters(name = "Executing: {0}, {1}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (IndexType type : IndexType.values()) {
            res.add(new Object[] { new StaticMapMemberHandler(), type });
            res.add(new Object[] { new DynamicMapMemberHandler(), type });
            res.add(new Object[] { new DynamicIndexMemberHandler(), type });
            res.add(new Object[] { new DynamicMapClientHandler(), type });
            res.add(new Object[] { new DynamicIndexClientHandler(), type });
        }

        return res;
    }

    @Parameterized.Parameter(0)
    public static Handler handler;

    @Parameterized.Parameter(1)
    public static IndexType type;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testNoAttributes() {
        checkIndexFailed(IllegalArgumentException.class, "Index must have at least one attribute",
            createConfig());
    }

    @Test
    public void testTooManyAttributes() {
        IndexConfig config = new IndexConfig();

        for (int i = 0; i < IndexUtils.MAX_ATTRIBUTES + 1; i++) {
            config.addAttribute("col" + i);
        }

        checkIndexFailed(IllegalArgumentException.class,
            "Index cannot have more than " + IndexUtils.MAX_ATTRIBUTES + " attributes", config);
    }

    @Test
    public void testDuplicateAttributeName1() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate attribute name [attributeName=bad",
            createConfig("bad", "bad"));
    }

    @Test
    public void testDuplicateAttributeName2() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate attribute name [attributeName=bad",
            createConfig("good", "bad", "bad"));
    }

    @Test
    public void testDuplicateAttributeName3() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate attribute name [attributeName=bad",
            createConfig("bad", "good", "bad"));
    }

    @Test
    public void testDuplicateAttributeName4() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate attribute name [attributeName=bad",
            createConfig("bad", "bad", "good"));
    }

    @Test
    public void testDuplicateAttributeNameMasked1() {
        checkIndexFailed(IllegalArgumentException.class,
            "Duplicate attribute names [attributeName1=bad, attributeName2=this.bad", createConfig("bad", "this.bad"));
    }

    @Test
    public void testDuplicateAttributeNameMasked2() {
        checkIndexFailed(IllegalArgumentException.class,
            "Duplicate attribute names [attributeName1=this.bad, attributeName2=bad", createConfig("this.bad", "bad"));
    }

    @Test
    public void testDuplicateAttributeNameMasked3() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate attribute name [attributeName=this.bad",
            createConfig("this.bad", "this.bad"));
    }

    @Test
    public void testSingleAttribute() {
        checkIndex(createConfig("col1"), createConfig("this.col2"));
    }

    @Test
    public void testSingleAttributeWithName() {
        checkIndex(createNamedConfig("index", "col"), createNamedConfig("index2", "this.col2"));
    }

    @Test
    public void testMultipleAttributes() {
        if (type == IndexType.BITMAP) {
            thrown.expect(IllegalArgumentException.class);
            thrown.expectMessage(startsWith("Composite bitmap indexes are not supported:"));
        }
        checkIndex(createConfig("col1", "this.col2"));
    }

    @Test
    public void testMultipleAttributesWithName() {
        if (type == IndexType.BITMAP) {
            thrown.expect(IllegalArgumentException.class);
            thrown.expectMessage(startsWith("Composite bitmap indexes are not supported:"));
        }
        checkIndex(createNamedConfig("index", "col1", "this.col2"));
    }

    private void checkIndex(IndexConfig... indexConfigs) {
        List<HazelcastInstanceProxy> members = handler.initialize(hazelcastFactory, indexConfigs);

        for (HazelcastInstanceProxy member : members) {
            MapService service = member.getOriginal().node.nodeEngine.getService(MapService.SERVICE_NAME);
            MapServiceContext mapServiceContext = service.getMapServiceContext();
            MapContainer mapContainer = mapServiceContext.getMapContainer(MAP_NAME);

            Indexes indexes = mapContainer.getIndexes();

            assertEquals(indexConfigs.length, indexes.getIndexes().length);

            for (IndexConfig indexConfig : indexConfigs) {
                String expectedName = getExpectedName(indexConfig);

                InternalIndex index = indexes.getIndex(expectedName);

                assertNotNull("Index not found: " + expectedName, index);

                assertEquals(type == IndexType.SORTED, index.isOrdered());
                assertEquals(type, index.getConfig().getType());
                assertEquals(indexConfig.getAttributes().size(), index.getComponents().length);

                for (int i = 0; i < indexConfig.getAttributes().size(); i++) {
                    String expAttributeName = indexConfig.getAttributes().get(i);
                    String componentName = index.getComponents()[i];

                    assertEquals(IndexUtils.canonicalizeAttribute(expAttributeName), componentName);
                }
            }
        }
    }

    private void checkIndexFailed(Class<? extends Throwable> exceptionClass, String exceptionMessage,
        IndexConfig... indexConfigs) {
        thrown.expect(exceptionClass);
        thrown.expectMessage(startsWith(exceptionMessage));

        handler.initialize(hazelcastFactory, indexConfigs);
    }

    private static IndexConfig createNamedConfig(String name, String... attributes) {
        return createConfig(attributes).setName(name);
    }

    private static IndexConfig createConfig(String... attributes) {
        IndexConfig config = new IndexConfig();

        config.setType(type);

        if (attributes != null) {
            for (String attribute : attributes) {
                config.addAttribute(attribute);
            }
        }

        return config;
    }

    private static String getExpectedName(IndexConfig config) {
        if (config.getName() != null && !config.getName().trim().isEmpty()) {
            return config.getName();
        }

        StringBuilder res = new StringBuilder(MAP_NAME).append("_");

        if (config.getType() == IndexType.SORTED) {
            res.append("sorted");
        } else if (config.getType() == IndexType.HASH) {
            res.append("hash");
        } else if (config.getType() == IndexType.BITMAP) {
            res.append("bitmap");
        } else {
            throw new IllegalArgumentException("unexpected index type: " + config.getType());
        }

        for (String attribute : config.getAttributes()) {
            res.append("_").append(IndexUtils.canonicalizeAttribute(attribute));
        }

        return res.toString();
    }

    private interface Handler {
        List<HazelcastInstanceProxy> initialize(TestHazelcastFactory hazelcastFactory, IndexConfig... indexConfigs);
    }

    private static class StaticMapMemberHandler implements Handler {
        @Override
        public List<HazelcastInstanceProxy> initialize(TestHazelcastFactory hazelcastFactory,
            IndexConfig... indexConfigs) {
            MapConfig mapConfig = new MapConfig(MAP_NAME);

            for (IndexConfig indexConfig : indexConfigs) {
                mapConfig.addIndexConfig(indexConfig);
            }

            Config config = new Config().addMapConfig(mapConfig);

            HazelcastInstanceProxy member = (HazelcastInstanceProxy) hazelcastFactory.newHazelcastInstance(config);
            HazelcastInstanceProxy member2 = (HazelcastInstanceProxy) hazelcastFactory.newHazelcastInstance(config);

            member.getMap(MAP_NAME);

            return Arrays.asList(member, member2);
        }
    }

    private static class DynamicMapMemberHandler implements Handler {
        @Override
        public List<HazelcastInstanceProxy> initialize(TestHazelcastFactory hazelcastFactory,
            IndexConfig... indexConfigs) {
            Config config = new Config();

            HazelcastInstanceProxy member = (HazelcastInstanceProxy) hazelcastFactory.newHazelcastInstance(config);
            HazelcastInstanceProxy member2 = (HazelcastInstanceProxy) hazelcastFactory.newHazelcastInstance(config);

            MapConfig mapConfig = new MapConfig(MAP_NAME);

            for (IndexConfig indexConfig : indexConfigs) {
                mapConfig.addIndexConfig(indexConfig);
            }

            member.getConfig().addMapConfig(mapConfig);

            member.getMap(MAP_NAME);

            return Arrays.asList(member, member2);
        }
    }

    private static class DynamicIndexMemberHandler implements Handler {
        @Override
        public List<HazelcastInstanceProxy> initialize(TestHazelcastFactory hazelcastFactory,
            IndexConfig... indexConfigs) {
            Config config = new Config();

            HazelcastInstanceProxy member = (HazelcastInstanceProxy) hazelcastFactory.newHazelcastInstance(config);
            HazelcastInstanceProxy member2 = (HazelcastInstanceProxy) hazelcastFactory.newHazelcastInstance(config);

            IMap map = member.getMap(MAP_NAME);

            for (IndexConfig indexConfig : indexConfigs) {
                map.addIndex(indexConfig);
            }

            return Arrays.asList(member, member2);
        }
    }

    private static class DynamicMapClientHandler implements Handler {
        @Override
        public List<HazelcastInstanceProxy> initialize(TestHazelcastFactory hazelcastFactory,
            IndexConfig... indexConfigs) {
            Config config = new Config();

            HazelcastInstanceProxy member = (HazelcastInstanceProxy) hazelcastFactory.newHazelcastInstance(config);
            HazelcastInstanceProxy member2 = (HazelcastInstanceProxy) hazelcastFactory.newHazelcastInstance(config);
            HazelcastInstance client = hazelcastFactory.newHazelcastClient();

            MapConfig mapConfig = new MapConfig(MAP_NAME);

            for (IndexConfig indexConfig : indexConfigs) {
                mapConfig.addIndexConfig(indexConfig);
            }

            client.getConfig().addMapConfig(mapConfig);

            client.getMap(MAP_NAME);

            return Arrays.asList(member, member2);
        }
    }

    private static class DynamicIndexClientHandler implements Handler {
        @Override
        public List<HazelcastInstanceProxy> initialize(TestHazelcastFactory hazelcastFactory,
            IndexConfig... indexConfigs) {
            Config config = new Config();

            HazelcastInstanceProxy member = (HazelcastInstanceProxy) hazelcastFactory.newHazelcastInstance(config);
            HazelcastInstanceProxy member2 = (HazelcastInstanceProxy) hazelcastFactory.newHazelcastInstance(config);
            HazelcastInstance client = hazelcastFactory.newHazelcastClient();

            IMap map = client.getMap(MAP_NAME);

            for (IndexConfig indexConfig : indexConfigs) {
                map.addIndex(indexConfig);
            }

            return Arrays.asList(member, member2);
        }
    }
}
