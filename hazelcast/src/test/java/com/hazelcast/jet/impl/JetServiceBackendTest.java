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

package com.hazelcast.jet.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;

import static com.hazelcast.config.MapConfig.DEFAULT_BACKUP_COUNT;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_CATALOG_MAP_NAME;
import static com.hazelcast.jet.impl.JetServiceBackend.createSqlCatalogConfig;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetServiceBackendTest extends JetTestSupport {
    @Test
    public void when_instanceIsCreated_then_sqlCatalogIsConfigured() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        HazelcastInstance instance = createHazelcastInstance(config);
        MapConfig mapConfig = instance.getConfig().getMapConfig(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        assertEquals(createSqlCatalogConfig(), mapConfig);
    }

    @Test
    public void when_instanceIsCreatedWithOverriddenConfiguration_then_sqlCatalogConfigIsNotMerged() {
        Config config = new Config();
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        config.addMapConfig(getMapConfig(SQL_CATALOG_MAP_NAME, dataPersistenceConfig));
        config.getJetConfig().setEnabled(true);

        HazelcastInstance instance = createHazelcastInstance(config);
        MapConfig mapConfig = instance.getConfig().getMapConfig(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        assertEquals(new DataPersistenceConfig(), mapConfig.getDataPersistenceConfig());
        assertEquals(createSqlCatalogConfig(), mapConfig);

        MapConfig otherMapConfig = ((MapProxyImpl) instance.getMap("otherMap")).getMapConfig();
        assertFalse(otherMapConfig.getDataPersistenceConfig().isEnabled());
    }

    @Test
    public void when_instanceIsCreatedWithOverriddenDefaultConfiguration_then_sqlCatalogConfigIsNotMerged() {
        Config config = new Config();
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        config.addMapConfig(getMapConfig("default", dataPersistenceConfig));
        config.getJetConfig().setEnabled(true);

        HazelcastInstance instance = createHazelcastInstance(config);
        MapConfig mapConfig = instance.getConfig().getMapConfig(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        assertEquals(new DataPersistenceConfig(), mapConfig.getDataPersistenceConfig());
        assertEquals(createSqlCatalogConfig(), mapConfig);
    }

    @Test
    public void wrapFileNotFoundException() {
        String message = "file not found";
        FileNotFoundException fileNotFound = new FileNotFoundException(message);
        assertThatThrownBy(() -> JetServiceBackend.wrapWithJetException(fileNotFound))
                .isInstanceOf(JetException.class)
                .hasRootCauseInstanceOf(FileNotFoundException.class)
                .hasMessageContaining(message);
    }

    @Test
    public void wrapJetException() {
        String message = "jet exception";
        JetException jetException = new JetException(message);
        assertThatThrownBy(() -> JetServiceBackend.wrapWithJetException(jetException))
                .isInstanceOf(JetException.class)
                .hasNoCause()
                .hasMessageContaining(message);
    }

    @Test
    public void when_instanceIsCreatedWithOverriddenDefaultConfiguration_then_defaultConfigurationIsNotChanged() {
        Config config = new Config();
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        config.addMapConfig(getMapConfig("default", dataPersistenceConfig));
        config.getJetConfig().setEnabled(true);

        HazelcastInstance instance = createHazelcastInstance(config);
        MapConfig otherMapConfig = ((MapProxyImpl) instance.getMap("otherMap")).getMapConfig();
        assertTrue(otherMapConfig.getDataPersistenceConfig().isEnabled());
        assertEquals(DEFAULT_BACKUP_COUNT, otherMapConfig.getBackupCount());
    }

    @Test
    public void when_instanceIsCreatedWithOverriddenDefaultWildcardConfiguration_then_sqlCatalogConfigIsNotMerged() {
        Config config = new Config();
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        config.addMapConfig(getMapConfig("*", dataPersistenceConfig));
        config.getJetConfig().setEnabled(true);

        HazelcastInstance instance = createHazelcastInstance(config);
        MapConfig mapConfig = instance.getConfig().getMapConfig(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        assertEquals(new DataPersistenceConfig(), mapConfig.getDataPersistenceConfig());
        assertEquals(createSqlCatalogConfig(), mapConfig);
    }

    @Test
    public void when_instanceIsCreatedWithOverriddenDefaultWildcardConfiguration_then_defaultConfigurationIsNotChanged() {
        Config config = new Config();
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        config.addMapConfig(getMapConfig("*", dataPersistenceConfig));
        config.getJetConfig().setEnabled(true);

        HazelcastInstance instance = createHazelcastInstance(config);
        MapConfig otherMapConfig = ((MapProxyImpl) instance.getMap("otherMap")).getMapConfig();
        assertTrue(otherMapConfig.getDataPersistenceConfig().isEnabled());
        assertEquals(DEFAULT_BACKUP_COUNT, otherMapConfig.getBackupCount());
    }

    private static MapConfig getMapConfig(String mapName, DataPersistenceConfig dataPersistenceConfig) {
        return new MapConfig(mapName).setDataPersistenceConfig(dataPersistenceConfig);
    }
}
