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

package com.hazelcast.spring.java;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.map.IMap;
import com.hazelcast.spring.CustomSpringExtension;
import com.hazelcast.sql.SqlService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
public abstract class AppContextTestBase {

    @Autowired
    private HazelcastInstance instance;

    @Autowired
    private JetService jet;

    @Autowired
    private SqlService sqlService;

    @Autowired(required = false)
    @Qualifier(value = "map1")
    protected IMap<String, String> map1;

    @Autowired(required = false)
    @Qualifier(value = "testMap")
    protected IMap<String, String> testMap;

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void testServices() {
        assertThat(instance).isNotNull();
        assertThat(jet).isNotNull();
        assertThat(sqlService).isNotNull();
    }

    @Test
    void testMap() {
        assertThat((Object) testMap).isNotNull();
        testMap.set("key1", "value1");
        assertEqualsEventually(() -> testMap.get("key1"), "value1");
    }

}
