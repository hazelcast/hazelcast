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

package com.hazelcast.spring.vector;

import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringExtension;
import com.hazelcast.vector.VectorCollection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static com.hazelcast.config.vector.Metric.DOT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@EnabledIfSystemProperty(named = "vector.module", matches = "true")
@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"vector-application-context.xml"})
public class VectorCollectionConfigBeanTest {

    @Autowired
    @Qualifier("vector-collection")
    private VectorCollection<?, ?> vectorCollection;

    @Autowired
    private HazelcastInstance instance;

    @Test
    public void shouldExposeVectorCollectionBean() {
        assertNotNull(instance);
        assertNotNull(vectorCollection);
        var expected = new VectorCollectionConfig("vector-collection")
            .addVectorIndexConfig(new VectorIndexConfig().setMetric(DOT).setDimension(1));
        var actual = instance.getConfig().getVectorCollectionConfigOrNull(vectorCollection.getName());
        assertEquals(expected, actual);
    }
}
