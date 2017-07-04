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

package com.hazelcast.spi;

import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.spi.PostJoinAwareOperation.Sequence.POST_JOIN_OPS_SEQUENCE;
import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PostJoinAwareOperationTest {

    @Test
    public void testAllPostJoinAwareOperation_haveSequenceDefinedInMap()
            throws Exception {
        Set<Class<? extends PostJoinAwareOperation>> postJoinAwareOps = REFLECTIONS.getSubTypesOf(PostJoinAwareOperation.class);
        for (Class<? extends PostJoinAwareOperation> op : postJoinAwareOps) {
            PostJoinAwareOperation operationInstance = ClassLoaderUtil.newInstance(op, null, op.getName());
            assertEquals(format("%s.getSequence() returned an unexpected value", op.getName()),
                    (int) POST_JOIN_OPS_SEQUENCE.get(op), operationInstance.getSequence());
        }
    }

    @Test
    public void testEachPostJoinAwareOperation_haveUniqueSequence() {
        Set<Class<? extends PostJoinAwareOperation>> postJoinAwareOps = REFLECTIONS.getSubTypesOf(PostJoinAwareOperation.class);
        Set<Integer> definedSequences = new HashSet<Integer>();
        for (Integer sequence : POST_JOIN_OPS_SEQUENCE.values()) {
            definedSequences.add(sequence);
        }
        assertEquals(postJoinAwareOps.size(), definedSequences.size());
    }

}