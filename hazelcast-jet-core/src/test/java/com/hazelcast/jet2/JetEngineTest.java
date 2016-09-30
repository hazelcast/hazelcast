/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
@Ignore
public class JetEngineTest extends HazelcastTestSupport {

    private static TestHazelcastInstanceFactory factory;

    @BeforeClass
    public static void before() {
        factory = new TestHazelcastInstanceFactory();
    }

    @AfterClass
    public static void after() {
        factory.shutdownAll();
    }

    @Test
    public void test() {
        HazelcastInstance instance = factory.newHazelcastInstance();
        factory.newHazelcastInstance();
        JetEngine jetEngine = JetEngine.get(instance, "jetEngine");

        DAG dag = new DAG();
        Job job = jetEngine.newJob(dag);

        job.execute();
    }

}
