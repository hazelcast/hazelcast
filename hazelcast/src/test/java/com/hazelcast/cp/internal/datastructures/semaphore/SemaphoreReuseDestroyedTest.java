/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SemaphoreReuseDestroyedTest extends AbstractSemaphoreDestroyedTest {

    @Override
    protected HazelcastInstance[] createInstances() {
        return newInstances(3);
    }

    @Override
    protected String getName() {
        return "semaphore@group";
    }

    protected ISemaphore createSemaphore(String name) {
        HazelcastInstance instance = instances[RandomPicker.getInt(instances.length)];
        return instance.getCPSubsystem().getSemaphore(name);
    }

}
