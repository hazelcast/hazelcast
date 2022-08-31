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

package com.hazelcast.jet.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.JetServiceBackend.SQL_CATALOG_MAP_CONFIG;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetServiceBackendTest extends JetTestSupport {
    private HazelcastInstance instance;

    @Before
    public void setup() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);

        instance = createHazelcastInstance(config);
    }

    @Test
    public void when_instanceIsCreated_then_sqlCatalogIsConfigured() {
        MapConfig mapConfig = instance.getConfig().getMapConfig(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        assertEquals(SQL_CATALOG_MAP_CONFIG, mapConfig);
    }
}
