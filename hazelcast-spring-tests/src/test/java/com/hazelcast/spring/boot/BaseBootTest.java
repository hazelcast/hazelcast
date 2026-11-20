/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.spring.boot;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.map.IMap;
import com.hazelcast.spring.AutowireHz;
import com.hazelcast.sql.SqlService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;

import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseBootTest {

    @AutowireHz
    protected List<HazelcastInstance> instance;

    @AutowireHz
    protected JetService jet;

    @AutowireHz
    protected SqlService sqlService;

    @AutowireHz
    @Qualifier(value = "map1")
    protected IMap<String, String> map1;

    @AutowireHz
    protected ApplicationContext applicationContext;

    @Test
    void testServices() {
        assertThat(jet).isNotNull();
        assertThat(sqlService).isNotNull();
    }

    @Test
    void testMap() {
        assertThat((Object) map1).isNotNull();
        map1.set("key1", "value1");
        assertTrueEventually(() -> assertThat(map1.get("key1")).isEqualTo("value1"));

        var firstInstance = instance.get(0);
        firstInstance.getConfig().addMapConfig(new MapConfig("dynamicMap"));

        String dynamicMapName = "dynamicMap";
        IMap<Integer, Integer> dynamicMap = firstInstance.getMap(dynamicMapName);
        dynamicMap.put(1, 2);
        assertTrueEventually(() -> assertThat(dynamicMap.get(1)).isEqualTo(2));
        assertThat(firstInstance.getConfig().getMapConfigs().values())
                .extracting(MapConfig::getName)
                .contains("map1", dynamicMapName);
    }
}
