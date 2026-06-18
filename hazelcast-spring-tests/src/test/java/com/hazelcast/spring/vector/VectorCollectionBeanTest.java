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

import com.hazelcast.spring.CustomSpringExtension;
import com.hazelcast.spring.java.SpringHazelcastConfiguration;
import com.hazelcast.vector.VectorCollection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.Assert.assertNotNull;

@EnabledIfSystemProperty(named = "vector.module", matches = "true")
@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(classes = SpringHazelcastConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class VectorCollectionBeanTest {

    @Autowired
    @Qualifier("vectors")
    private VectorCollection<?, ?> vectorCollection;

    @Test
    public void shouldExposeVectorCollectionBean() {
        assertNotNull(vectorCollection);
    }
}
