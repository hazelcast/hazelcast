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

package com.hazelcast.jet.stream.impl.distributed;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class DistributedLongSummaryStatisticsTest {

    @Test
    public void writeData() throws IOException {
        // Given
        DistributedLongSummaryStatistics stats = new DistributedLongSummaryStatistics();
        stats.accept(1);
        stats.accept(3);
        ObjectDataOutput out = mock(ObjectDataOutput.class);

        // When
        stats.writeData(out);

        // Then
        verify(out).writeLong(stats.getCount());
        verify(out).writeLong(stats.getSum());
        verify(out).writeLong(stats.getMin());
        verify(out).writeLong(stats.getMax());
        verifyNoMoreInteractions(out);
    }

    @Test
    public void readData() throws IOException {
        // Given
        DistributedLongSummaryStatistics stats = new DistributedLongSummaryStatistics();
        ObjectDataInput in = mock(ObjectDataInput.class);
        when(in.readLong()).thenReturn(1L)
                           .thenReturn(2L)
                           .thenReturn(3L)
                           .thenReturn(4L);

        // When
        stats.readData(in);

        // Then
        assertEquals(1, stats.getCount());
        assertEquals(2, stats.getSum());
        assertEquals(3, stats.getMin());
        assertEquals(4, stats.getMax());
    }
}
