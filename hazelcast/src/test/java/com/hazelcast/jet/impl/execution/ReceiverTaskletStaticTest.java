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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.execution.ReceiverTasklet.COMPRESSED_SEQ_UNIT_LOG2;
import static com.hazelcast.jet.impl.execution.ReceiverTasklet.compressSeq;
import static com.hazelcast.jet.impl.execution.ReceiverTasklet.estimatedMemoryFootprint;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReceiverTaskletStaticTest {

    @Test
    public void testCompressSeq() {
        assertEquals(0, compressSeq(0));
        assertEquals(0, compressSeq((1 << COMPRESSED_SEQ_UNIT_LOG2) - 1));
        assertEquals(1, compressSeq(1 << COMPRESSED_SEQ_UNIT_LOG2));
        long outsideIntRange = ((long) Integer.MAX_VALUE + 1) << COMPRESSED_SEQ_UNIT_LOG2;
        assertEquals(Integer.MIN_VALUE, compressSeq(outsideIntRange));
    }

    @Test
    public void testEstimatedMemoryFootprint() {
        assertEquals(56, estimatedMemoryFootprint(0));
        assertEquals(66, estimatedMemoryFootprint(10));
    }
}
