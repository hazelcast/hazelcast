/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.partition;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.DataInput;
import java.io.DataOutput;

import static com.hazelcast.spi.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.spi.partition.MigrationEndpoint.SOURCE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationEndpointTest {

    private DataOutput dataOutput = mock(DataOutput.class);
    private DataInput dataInput = mock(DataInput.class);

    @Test
    public void testWriteToSource() throws Exception {
        MigrationEndpoint.writeTo(SOURCE, dataOutput);

        verify(dataOutput).writeByte(0);
        verifyNoMoreInteractions(dataOutput);
    }

    @Test
    public void testWriteToDestination() throws Exception {
        MigrationEndpoint.writeTo(DESTINATION, dataOutput);

        verify(dataOutput).writeByte(1);
        verifyNoMoreInteractions(dataOutput);
    }

    @Test
    public void testReadFromSource() throws Exception {
        when(dataInput.readByte()).thenReturn((byte) 0);

        MigrationEndpoint actual = MigrationEndpoint.readFrom(dataInput);

        assertEquals(SOURCE, actual);
    }

    @Test
    public void testReadFromDestination() throws Exception {
        when(dataInput.readByte()).thenReturn((byte) 1);

        MigrationEndpoint actual = MigrationEndpoint.readFrom(dataInput);

        assertEquals(DESTINATION, actual);
    }
}
