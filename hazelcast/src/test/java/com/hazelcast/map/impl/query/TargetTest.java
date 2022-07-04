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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RootCauseMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TargetTest {

    @Rule
    public ExpectedException rule = ExpectedException.none();

    @Test
    public void testConstructor_withInvalidPartitionId() throws Exception {
        // retrieve the wanted constructor and make it accessible
        Constructor<Target> constructor = Target.class.getDeclaredConstructor(Target.TargetMode.class, PartitionIdSet.class);
        constructor.setAccessible(true);

        // we expect an IllegalArgumentException to be thrown
        rule.expect(new RootCauseMatcher(IllegalArgumentException.class));
        constructor.newInstance(Target.TargetMode.PARTITION_OWNER, null);
    }
}
