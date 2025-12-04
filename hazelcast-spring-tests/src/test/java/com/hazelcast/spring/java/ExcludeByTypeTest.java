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
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.map.IMap;
import com.hazelcast.spring.ConfigCreator;
import com.hazelcast.spring.CustomSpringExtension;
import com.hazelcast.spring.ExposeHazelcastObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(classes = ExcludeByTypeTest.UsingExcludeByType.class)
@EnableAutoConfiguration
public class ExcludeByTypeTest extends AppContextTestBase {

    @Test
    @Override
    public void testMap() {
        assertThat((Object) map1).isNull();
        assertThat((Object) testMap).isNull();
        assertThat((Object) ringbuffer).isNotNull();
    }

    @ExposeHazelcastObjects(excludeByName = "test", excludeByType = IMap.class)
    @ComponentScan(basePackages = "com.hazelcast.non.existing")
    @EnableAutoConfiguration
    public static class UsingExcludeByType {
        @Bean(destroyMethod = "shutdown")
        public HazelcastInstance hazelcastInstance() {
            return HazelcastInstanceFactory.newHazelcastInstance(ConfigCreator.createConfigWithStructures());
        }
    }
}
