/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.Mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.internal.cluster.Versions.V3_11;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DeleteOperationWanFlagSerializationTest {

    static final String MAP_NAME = "map";

    @Parameters(name = "clusterVersion: {0} byteOrder:{1} disableWanReplication:{2}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{
                {V3_10, ByteOrder.LITTLE_ENDIAN, true},
                {V3_10, ByteOrder.LITTLE_ENDIAN, false},
                {V3_10, ByteOrder.BIG_ENDIAN, true},
                {V3_10, ByteOrder.BIG_ENDIAN, false},

                {V3_11, ByteOrder.LITTLE_ENDIAN, true},
                {V3_11, ByteOrder.LITTLE_ENDIAN, false},
                {V3_11, ByteOrder.BIG_ENDIAN, true},
                {V3_11, ByteOrder.BIG_ENDIAN, false},
                });
    }

    @Parameter
    public Version version;

    @Parameter(1)
    public ByteOrder byteOrder;

    @Parameter(2)
    public boolean disableWanReplication;

    @Mock
    Data keyMock;

    @Mock
    private InternalSerializationService serializationServiceMock;

    @Before
    public void setUp() {
        initMocks(this);
        when(serializationServiceMock.getByteOrder()).thenReturn(byteOrder);
    }

    @Test
    public void testDeleteOperation() throws IOException {
        DeleteOperation original = new DeleteOperation(MAP_NAME, keyMock, disableWanReplication);
        DeleteOperation deserialized = new DeleteOperation();

        testSerialization(original, deserialized);
    }

    private void testSerialization(DeleteOperation originalOp, DeleteOperation deserializedOp) throws IOException {
        serializeAndDeserialize(originalOp, deserializedOp);

        assertEquals(originalOp.disableWanReplicationEvent, deserializedOp.disableWanReplicationEvent);
    }

    void serializeAndDeserialize(Operation originalOp, Operation deserializedOp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectDataOutputStream out = new ObjectDataOutputStream(baos, serializationServiceMock);
        out.setVersion(version);
        originalOp.writeData(out);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectDataInputStream in = new ObjectDataInputStream(bais, serializationServiceMock);
        in.setVersion(version);

        deserializedOp.readData(in);
    }

}
