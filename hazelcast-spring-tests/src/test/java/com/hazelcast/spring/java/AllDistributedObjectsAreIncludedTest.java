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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.spring.CustomSpringExtension;
import com.hazelcast.spring.HazelcastObjectExtractionConfiguration;
import com.hazelcast.transaction.TransactionalObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Set;

import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(classes = SpringHazelcastConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class AllDistributedObjectsAreIncludedTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AllDistributedObjectsAreIncludedTest.class);

    /**
     * Classes that are either abstract/base interfaces or structures that doesn't have configuration.
     */
    private static final Set<Class<?>> EXCEPTIONS = Set.of(
            com.hazelcast.collection.ICollection.class,
            com.hazelcast.cp.IAtomicReference.class,
            com.hazelcast.cp.ICountDownLatch.class,
            com.hazelcast.cp.IAtomicLong.class,
            com.hazelcast.core.PrefixedDistributedObject.class,
            com.hazelcast.multimap.BaseMultiMap.class,
            com.hazelcast.collection.BaseQueue.class,
            com.hazelcast.transaction.HazelcastXAResource.class,
            com.hazelcast.map.BaseMap.class,
            com.hazelcast.cache.impl.ICacheInternal.class);

    @Autowired
    private HazelcastObjectExtractionConfiguration conf;

    @AfterAll
    public static void afterAll() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    void checkAllDistributedObjectsAreIncluded() {
        Set<Class<?>> typesOfBeans = conf.registeredTypesOfBeans();
        LOGGER.debug("List of all registered: {}\n",  typesOfBeans.stream()
                                                                  .map(c -> " - " + c.getName())
                                                                  .collect(joining(System.lineSeparator())));

        Set<Class<? extends  DistributedObject>> typesOfDistributedObjects = REFLECTIONS.getSubTypesOf(DistributedObject.class);
        typesOfDistributedObjects.removeIf(c -> !c.isInterface());
        typesOfDistributedObjects.removeIf(c -> c.getName().startsWith("Base"));
        typesOfDistributedObjects.removeIf(EXCEPTIONS::contains);
        typesOfDistributedObjects.removeIf(TransactionalObject.class::isAssignableFrom);

        assertThat(typesOfBeans)
                .as("Some classes are not added as bean by Hazelcast configuration")
                .containsAll(typesOfDistributedObjects);
    }
}
