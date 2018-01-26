/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.hazelcast.util.SetUtil.createHashSet;
import static com.hazelcast.util.SetUtil.createLinkedHashSet;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SetUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(SetUtil.class);
    }

    @Test
    public void testCreateHashSet() {
        Set set = createHashSet(5);
        assertInstanceOf(HashSet.class, set);
    }

    @Test
    public void testCreatedLinkedHashSet() {
        Set set = createLinkedHashSet(5);
        assertInstanceOf(LinkedHashSet.class, set);
    }
}
