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

package com.hazelcast.config;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionOptions;
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

import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.startsWith;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexCreateTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "map";

    @Parameterized.Parameters(name = "Executing: {0}")
    public static Collection<Object[]> parameters() {

        TransactionOptions onePhaseOption = TransactionOptions.getDefault();
        onePhaseOption.setTransactionType(ONE_PHASE);

        TransactionOptions twoPhaseOption = TransactionOptions.getDefault();
        twoPhaseOption.setTransactionType(TWO_PHASE);

        List<Object[]> res = new ArrayList<>();

        res.add(new Object[] { new StaticMapMemberHandler() });
        res.add(new Object[] { new DynamicMapMemberHandler() });
        res.add(new Object[] { new DynamicIndexMemberHandler() });
        res.add(new Object[] { new DynamicMapClientHandler() });
        res.add(new Object[] { new DynamicIndexClientHandler() });

        return res;
    }

    @Parameterized.Parameter
    public static Handler handler;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testNoColumns() {
        checkIndexFailed(IllegalArgumentException.class, "Index must have at least one column",
            createConfig());
    }

    @Test
    public void testTooManyColumns() {
        IndexConfig config = new IndexConfig();

        for (int i = 0; i < IndexUtils.MAX_COLUMNS + 1; i++) {
            config.addColumn("col" + i);
        }

        checkIndexFailed(IllegalArgumentException.class,
            "Index cannot have more than " + IndexUtils.MAX_COLUMNS + " columns", config);
    }

    @Test
    public void testDuplicateColumnName1() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate column name [columnName=bad",
            createConfig("bad", "bad"));
    }

    @Test
    public void testDuplicateColumnName2() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate column name [columnName=bad",
            createConfig("good", "bad", "bad"));
    }

    @Test
    public void testDuplicateColumnName3() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate column name [columnName=bad",
            createConfig("bad", "good", "bad"));
    }

    @Test
    public void testDuplicateColumnName4() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate column name [columnName=bad",
            createConfig("bad", "bad", "good"));
    }

    @Test
    public void testDuplicateColumnNameMasked1() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate column names [columnName1=bad, columnName2=this.bad",
            createConfig("bad", "this.bad"));
    }

    @Test
    public void testDuplicateColumnNameMasked2() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate column names [columnName1=this.bad, columnName2=bad",
            createConfig("this.bad", "bad"));
    }

    @Test
    public void testDuplicateColumnNameMasked3() {
        checkIndexFailed(IllegalArgumentException.class, "Duplicate column name [columnName=this.bad",
            createConfig("this.bad", "this.bad"));
    }

    @Test
    public void testSingleColumn() {
        checkIndex(createConfig("col1"), createConfig("this.col2"));
    }

    @Test
    public void testSingleColumnWithName() {
        checkIndex(createNamedConfig("index", "col"), createNamedConfig("index2", "this.col2"));
    }

    @Test
    public void testMultipleColumns() {
        checkIndex(createConfig("col1, this.col2"));
    }

    @Test
    public void testMultipleColumnsWithName() {
        checkIndex(createNamedConfig("index", "col1, this.col2"));
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

                assertEquals(indexConfig.getType() == IndexType.SORTED, index.isOrdered());
                assertEquals(indexConfig.getColumns().size(), index.getComponents().size());

                for (int i = 0; i < indexConfig.getColumns().size(); i++) {
                    IndexColumnConfig expColumn = indexConfig.getColumns().get(i);
                    String componentName = index.getComponents().get(i);

                    assertEquals(IndexUtils.canonicalizeAttribute(expColumn.getName()), componentName);
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

    private static IndexConfig createNamedConfig(String name, String... columns) {
        return createConfig(columns).setName(name);
    }

    private static IndexConfig createConfig(String... columns) {
        IndexConfig config = new IndexConfig();

        if (columns != null) {
            for (String column : columns) {
                config.addColumn(column);
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
        } else {
            res.append("hash");
        }

        for (IndexColumnConfig column : config.getColumns()) {
            res.append("_").append(IndexUtils.canonicalizeAttribute(column.getName()));
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
