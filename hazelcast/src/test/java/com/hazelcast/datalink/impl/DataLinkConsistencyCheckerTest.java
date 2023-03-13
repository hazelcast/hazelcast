/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datalink.impl;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.datalink.DataLink;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DataLinkConsistencyCheckerTest extends SimpleTestInClusterSupport {

    private DataLinkServiceImpl linkService;
    private IMap<Object, Object> sqlCatalog;
    private DataLinkConsistencyChecker dataLinkConsistencyChecker;

    private String name = "dl";
    private String type = DataLinkTestUtil.DummyDataLink.class.getName();

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Before
    public void setUp() throws Exception {
        linkService = (DataLinkServiceImpl) getNodeEngineImpl(instance()).getDataLinkService();
        sqlCatalog = instance().getMap(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        dataLinkConsistencyChecker = new DataLinkConsistencyChecker(instance(), Util.getNodeEngine(instance()));
        dataLinkConsistencyChecker.init();
    }

    @After
    public void tearDown() throws Exception {
        linkService.removeDataLink(name);
    }

    @Test
    public void test_missingDataLinkWasAddedToSqlCatalog() {
        linkService.createSqlDataLink(name, type, Collections.emptyMap(), false);
        assertFalse(sqlCatalog.containsKey(QueryUtils.wrapDataLinkKey(name)));
        dataLinkConsistencyChecker.check();
        assertTrue(sqlCatalog.containsKey(QueryUtils.wrapDataLinkKey(name)));

        Object obj = sqlCatalog.get(QueryUtils.wrapDataLinkKey(name));
        assertInstanceOf(DataLink.class, obj);

        DataLinkConfig catalogDataLinkConfig = linkService.toConfig(name, type, Collections.emptyMap());
        com.hazelcast.datalink.DataLink dataLink = null;
        try {
            dataLink = linkService.getAndRetainDataLink(name, com.hazelcast.datalink.DataLink.class);
            assertEquals(catalogDataLinkConfig, dataLink.getConfig());
        } finally {
            assertNotNull(dataLink);
            dataLink.retain();
        }
    }

    @Test
    public void test_outdatedDataLinkWasAlteredInSqlCatalog() {
        linkService.createSqlDataLink(name, type, Collections.emptyMap(), false);
        sqlCatalog.put(QueryUtils.wrapDataLinkKey(name), new DataLink(name, type, Collections.emptyMap()));
        assertTrue(sqlCatalog.containsKey(QueryUtils.wrapDataLinkKey(name)));

        linkService.removeDataLink(name);
        Map<String, String> newOptions = singletonMap("a", "b");
        linkService.createSqlDataLink(name, type, newOptions, false);
        dataLinkConsistencyChecker.check();

        DataLinkConfig catalogDataLinkConfig = linkService.toConfig(name, type, newOptions);
        com.hazelcast.datalink.DataLink dataLink = null;
        try {
            dataLink = linkService.getAndRetainDataLink(name, com.hazelcast.datalink.DataLink.class);
            assertEquals(catalogDataLinkConfig, dataLink.getConfig());
        } finally {
            assertNotNull(dataLink);
            dataLink.retain();
        }
    }

    @Test
    public void test_outdatedDataLinkWasRemovedFromSqlCatalog() {
        linkService.createSqlDataLink(name, type, Collections.emptyMap(), false);
        sqlCatalog.put(QueryUtils.wrapDataLinkKey(name), new DataLink(name, type, Collections.emptyMap()));
        assertTrue(sqlCatalog.containsKey(QueryUtils.wrapDataLinkKey(name)));

        linkService.removeDataLink(name);
        dataLinkConsistencyChecker.check();
        assertFalse(sqlCatalog.containsKey(QueryUtils.wrapDataLinkKey(name)));
    }

    @Test
    public void test_addEntryToCatalog() {
        linkService.createSqlDataLink(name, type, Collections.emptyMap(), false);
        com.hazelcast.datalink.DataLink dataLink = linkService.getAndRetainDataLink(name,
                com.hazelcast.datalink.DataLink.class);
        DataLinkServiceImpl.DataLinkSourcePair pair = new DataLinkServiceImpl.DataLinkSourcePair(
                dataLink,
                DataLinkServiceImpl.DataLinkSource.SQL);

        dataLinkConsistencyChecker.addEntryToCatalog(singletonMap(name, pair).entrySet().iterator().next());
        assertTrue(sqlCatalog.containsKey(QueryUtils.wrapDataLinkKey(name)));
    }
}
