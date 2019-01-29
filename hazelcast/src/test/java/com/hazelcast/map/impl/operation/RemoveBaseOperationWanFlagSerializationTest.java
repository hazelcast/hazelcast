package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class RemoveBaseOperationWanFlagSerializationTest {

    static final String MAP_NAME = "map";

    @Parameters(name = "byteOrder:{0} disableWanReplication:{1}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{
                {ByteOrder.LITTLE_ENDIAN, true},
                {ByteOrder.LITTLE_ENDIAN, false},
                {ByteOrder.BIG_ENDIAN, true},
                {ByteOrder.BIG_ENDIAN, false},
                });
    }

    @Parameter
    public ByteOrder byteOrder;

    @Parameter(1)
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
    public void testRemoveOperation() throws IOException {
        BaseRemoveOperation original = new RemoveOperation(MAP_NAME, keyMock, disableWanReplication);
        BaseRemoveOperation deserialized = new RemoveOperation();

        testSerialization(original, deserialized);
    }

    @Test
    public void testDeleteOperation() throws IOException {
        BaseRemoveOperation original = new DeleteOperation(MAP_NAME, keyMock, disableWanReplication);
        BaseRemoveOperation deserialized = new DeleteOperation();

        testSerialization(original, deserialized);
    }

    private void testSerialization(BaseRemoveOperation originalOp, BaseRemoveOperation deserializedOp) throws IOException {
        serializeAndDeserialize(originalOp, deserializedOp);

        assertEquals(originalOp.disableWanReplicationEvent, deserializedOp.disableWanReplicationEvent);
    }

    void serializeAndDeserialize(Operation originalOp, Operation deserializedOp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectDataOutputStream out = new ObjectDataOutputStream(baos, serializationServiceMock);
        originalOp.writeData(out);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectDataInputStream in = new ObjectDataInputStream(bais, serializationServiceMock);
        deserializedOp.readData(in);
    }

}
