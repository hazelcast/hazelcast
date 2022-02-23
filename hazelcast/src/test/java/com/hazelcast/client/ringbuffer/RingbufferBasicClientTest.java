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

package com.hazelcast.client.ringbuffer;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.impl.RingbufferAbstractTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.function.Function;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferBasicClientTest extends RingbufferAbstractTest {

    private static TestHazelcastFactory factory = new TestHazelcastFactory();

    @BeforeClass
    public static void beforeClass() throws Exception {
        Function<Config, HazelcastInstance[]> instanceCreator = config -> {
            HazelcastInstance member = factory.newHazelcastInstance(config);
            HazelcastInstance client = factory.newHazelcastClient();
            return new HazelcastInstance[]{client, member};
        };

        prepare(instanceCreator);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        factory.terminateAll();
    }
}
