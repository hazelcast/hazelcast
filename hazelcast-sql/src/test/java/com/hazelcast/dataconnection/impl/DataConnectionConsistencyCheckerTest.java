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

package com.hazelcast.dataconnection.impl;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.DataConnectionConsistencyChecker;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;
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
public class DataConnectionConsistencyCheckerTest extends SimpleTestInClusterSupport {

    private DataConnectionServiceImpl linkService;
    private IMap<Object, Object> sqlCatalog;
    private DataConnectionConsistencyChecker dataConnectionConsistencyChecker;

    private String name;
    private final String type = "dummy";

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(3, null);
    }

    @Before
    public void setUp() throws Exception {
        name = randomName();
        linkService = (DataConnectionServiceImpl) getNodeEngineImpl(instance()).getDataConnectionService();
        sqlCatalog = instance().getMap(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        dataConnectionConsistencyChecker = new DataConnectionConsistencyChecker(instance(), Util.getNodeEngine(instance()));
        dataConnectionConsistencyChecker.init();
    }

    @After
    public void tearDown() throws Exception {
        if (linkService.existsSqlDataConnection(name)) {
            linkService.removeDataConnection(name);
        }
    }

    @Test
    public void test_missingDataConnectionWasAddedToDataConnectionService() {
        assertFalse(linkService.existsSqlDataConnection(name));
        sqlCatalog.put(
                QueryUtils.wrapDataConnectionKey(name),
                new DataConnectionCatalogEntry(name, type, false, Collections.emptyMap()));
        dataConnectionConsistencyChecker.check();
        assertTrue(linkService.existsSqlDataConnection(name));

        DataConnectionConfig catalogDataConnectionConfig = linkService.toConfig(name, type, false, Collections.emptyMap());
        DataConnection dataConnection = null;
        try {
            dataConnection = linkService.getAndRetainDataConnection(name, DataConnection.class);
            assertEquals(catalogDataConnectionConfig, dataConnection.getConfig());
        } finally {
            assertNotNull(dataConnection);
            dataConnection.retain();
        }
    }

    @Test
    public void test_outdatedDataConnectionWasAlteredInDataConnectionService() {
        // given
        linkService.createOrReplaceSqlDataConnection(name, type, false, Collections.emptyMap());
        Map<String, String> alteredOptions = singletonMap("a", "b");
        sqlCatalog.put(QueryUtils.wrapDataConnectionKey(name), new DataConnectionCatalogEntry(name, type, true, alteredOptions));

        DataConnectionConfig catalogDataConnectionConfig = linkService.toConfig(name, type, true, alteredOptions);

        // when
        dataConnectionConsistencyChecker.check();

        // then
        DataConnection dataConnection = null;
        try {
            dataConnection = linkService.getAndRetainDataConnection(name, DataConnection.class);
            assertEquals(catalogDataConnectionConfig, dataConnection.getConfig());
        } finally {
            assertNotNull(dataConnection);
            dataConnection.retain();
        }
    }

    @Test
    public void test_outdatedDataConnectionWasRemovedFromDataConnectionService() {
        // given
        linkService.createOrReplaceSqlDataConnection(name, type, false, Collections.emptyMap());
        assertTrue(linkService.existsSqlDataConnection(name));
        assertFalse(sqlCatalog.containsKey(QueryUtils.wrapDataConnectionKey(name)));

        // when
        dataConnectionConsistencyChecker.check();

        // then
        assertFalse(linkService.existsSqlDataConnection(name));
    }

    @Test
    public void test_dynamicConfigOriginatedDataConnectionWasAddedToDataConnectionService() {
        // given : data connection was created by SQL
        sqlCatalog.put(
                QueryUtils.wrapDataConnectionKey(name),
                new DataConnectionCatalogEntry(name, type, false, Collections.emptyMap()));
        linkService.createConfigDataConnection(new DataConnectionConfig(name).setType(type));
        assertTrueEventually(() -> linkService.existsConfigDataConnection(name));

        // when
        dataConnectionConsistencyChecker.check();

        // then-2 - dynamic config has higher priority, and __sql.catalog should NOT contain old version
        assertFalse(linkService.existsSqlDataConnection(name));
        assertFalse(sqlCatalog.containsKey(QueryUtils.wrapDataConnectionKey(name)));
    }
}
