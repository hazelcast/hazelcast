/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.datalink.impl;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.DataLink;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.DataLinkConsistencyChecker;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.datalink.DataLinkCatalogEntry;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataLinkConsistencyCheckerTest extends SimpleTestInClusterSupport {

    private DataLinkServiceImpl linkService;
    private IMap<Object, Object> sqlCatalog;
    private DataLinkConsistencyChecker dataLinkConsistencyChecker;

    private String name;
    private final String type = "dummy";

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(3, null);
    }

    @Before
    public void setUp() throws Exception {
        name = randomName();
        linkService = (DataLinkServiceImpl) getNodeEngineImpl(instance()).getDataLinkService();
        sqlCatalog = instance().getMap(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        dataLinkConsistencyChecker = new DataLinkConsistencyChecker(instance(), Util.getNodeEngine(instance()));
        dataLinkConsistencyChecker.init();
    }

    @After
    public void tearDown() throws Exception {
        if (linkService.existsSqlDataLink(name)) {
            linkService.removeDataLink(name);
        }
    }

    @Test
    public void test_missingDataLinkWasAddedToDataLinkService() {
        assertFalse(linkService.existsSqlDataLink(name));
        sqlCatalog.put(
                QueryUtils.wrapDataLinkKey(name),
                new DataLinkCatalogEntry(name, type, false, Collections.emptyMap()));
        dataLinkConsistencyChecker.check();
        assertTrue(linkService.existsSqlDataLink(name));

        DataLinkConfig catalogDataLinkConfig = linkService.toConfig(name, type, false, Collections.emptyMap());
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
    public void test_outdatedDataLinkWasAlteredInDataLinkService() {
        // given
        linkService.replaceSqlDataLink(name, type, false, Collections.emptyMap());
        Map<String, String> alteredOptions = singletonMap("a", "b");
        sqlCatalog.put(QueryUtils.wrapDataLinkKey(name), new DataLinkCatalogEntry(name, type, true, alteredOptions));

        DataLinkConfig catalogDataLinkConfig = linkService.toConfig(name, type, true, alteredOptions);

        // when
        dataLinkConsistencyChecker.check();

        // then
        DataLink dataLink = null;
        try {
            dataLink = linkService.getAndRetainDataLink(name, DataLink.class);
            assertEquals(catalogDataLinkConfig, dataLink.getConfig());
        } finally {
            assertNotNull(dataLink);
            dataLink.retain();
        }
    }

    @Test
    public void test_outdatedDataLinkWasRemovedFromDataLinkService() {
        // given
        linkService.replaceSqlDataLink(name, type, false, Collections.emptyMap());
        assertTrue(linkService.existsSqlDataLink(name));
        assertFalse(sqlCatalog.containsKey(QueryUtils.wrapDataLinkKey(name)));

        // when
        dataLinkConsistencyChecker.check();

        // then
        assertFalse(linkService.existsSqlDataLink(name));
    }

    @Test
    public void test_dynamicConfigOriginatedDataLinkWasAddedToDataLinkService() {
        // given : data link was created by SQL
        sqlCatalog.put(
                QueryUtils.wrapDataLinkKey(name),
                new DataLinkCatalogEntry(name, type, false, Collections.emptyMap()));
        linkService.createConfigDataLink(new DataLinkConfig(name).setType(type));
        assertTrueEventually(() -> linkService.existsConfigDataLink(name));

        // when
        dataLinkConsistencyChecker.check();

        // then-2 - dynamic config has higher priority, and __sql.catalog should NOT contain old version
        assertFalse(linkService.existsSqlDataLink(name));
        assertFalse(sqlCatalog.containsKey(QueryUtils.wrapDataLinkKey(name)));
    }
}
