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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueMigrationTest extends HazelcastTestSupport {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule
            = new ChangeLoggingRule("log4j2-debug-queue.xml");

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance ownerMember;
    private String queueName;

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    @Before
    public void setup() {
        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);
        factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] cluster = factory.newInstances(config);
        ownerMember = cluster[1];
        queueName = randomNameOwnedBy(ownerMember);
    }

    @Test
    public void testMigration() {
        testReplication(false);
    }

    @Test
    public void testPromotion() {
        testReplication(true);
    }

    private void testReplication(boolean terminateOwner) {
        List<VersionedObject<Integer>> expectedItems = new LinkedList<>();
        IQueue<Object> queue = getRandomInstance().getQueue(queueName);
        for (int i = 0; i < 100; i++) {
            queue.add(new VersionedObject<>(i, i));
            expectedItems.add(new VersionedObject<>(i, i));
        }

        if (terminateOwner) {
            TestUtil.terminateInstance(ownerMember);
        } else {
            ownerMember.shutdown();
        }

        queue = getRandomInstance().getQueue(queueName);
        assertEquals(expectedItems.size(), queue.size());

        getRandomInstance().shutdown();
        queue = getRandomInstance().getQueue(queueName);

        assertEquals(expectedItems.size(), queue.size());
        @SuppressWarnings("unchecked")
        VersionedObject<Integer>[] a = queue.toArray(new VersionedObject[0]);
        List<VersionedObject<Integer>> actualItems = Arrays.asList(a);
        assertEquals(expectedItems, actualItems);
    }

    private HazelcastInstance getRandomInstance() {
        HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[0]);
        Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)];
    }
}
