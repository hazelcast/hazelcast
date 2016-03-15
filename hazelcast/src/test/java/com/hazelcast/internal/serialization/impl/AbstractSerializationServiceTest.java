package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.CustomSerializationTest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractSerializationServiceTest {

    private AbstractSerializationService abstractSerializationService;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder = new DefaultSerializationServiceBuilder();
        abstractSerializationService = (AbstractSerializationService) defaultSerializationServiceBuilder
                .setVersion(InternalSerializationService.VERSION_1).build();
    }

    @Test
    public void testExternalizable() {
        ExternalizableValue original = new ExternalizableValue(100);

        Data data = abstractSerializationService.toData(original);
        ExternalizableValue found = abstractSerializationService.toObject(data);

        assertNotNull(found);
        assertEquals(original.value, found.value);
    }

    static class ExternalizableValue implements Externalizable {
        int value;

        ExternalizableValue() {
        }

        ExternalizableValue(int value) {
            this.value = value;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            value = in.readInt();
        }
    }

    @Test
    public void testSerializable() {
        SerializableleValue original = new SerializableleValue(100);

        Data data = abstractSerializationService.toData(original);
        SerializableleValue found = abstractSerializationService.toObject(data);

        assertNotNull(found);
        assertEquals(original.value, found.value);
    }

    static class SerializableleValue implements Serializable {
        int value;

        SerializableleValue(int value) {
            this.value = value;
        }
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testToBytesHandleThrowable() throws Exception {
        abstractSerializationService.register(StringBuffer.class, new StringBufferSerializer(true));
        abstractSerializationService.toBytes(new StringBuffer());
    }

    @Test
    public void testToObject_ServiceInactive() throws Exception {
        expectedException.expect(HazelcastSerializationException.class);
        expectedException.expectCause(Is.is(IsInstanceOf.<Throwable>instanceOf(HazelcastInstanceNotActiveException.class)));

        abstractSerializationService.register(StringBuffer.class, new StringBufferSerializer(false));
        Data data = abstractSerializationService.toData(new StringBuffer());
        abstractSerializationService.dispose();
        abstractSerializationService.toObject(data);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testWriteObject_serializerFail() throws Exception {
        abstractSerializationService.register(StringBuffer.class, new StringBufferSerializer(true));
        BufferObjectDataOutput out = abstractSerializationService.createObjectDataOutput();
        abstractSerializationService.writeObject(out, new StringBuffer());
    }

    @Test
    public void testReadObject_ServiceInactive() throws Exception {
        expectedException.expect(HazelcastSerializationException.class);
        expectedException.expectCause(Is.is(IsInstanceOf.<Throwable>instanceOf(HazelcastInstanceNotActiveException.class)));

        abstractSerializationService.register(StringBuffer.class, new StringBufferSerializer(false));
        Data data = abstractSerializationService.toData(new StringBuffer());
        abstractSerializationService.dispose();

        BufferObjectDataInput in = abstractSerializationService.createObjectDataInput(data);
        in.position(HeapData.TYPE_OFFSET);
        abstractSerializationService.readObject(in);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegister_nullType() throws Exception {
        abstractSerializationService.register(null, new StringBufferSerializer(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegister_typeIdNegative() throws Exception {
        StringBufferSerializer serializer = new StringBufferSerializer(true);
        serializer.typeId = -10000;
        abstractSerializationService.register(StringBuffer.class, serializer);
    }

    @Test(expected = IllegalStateException.class)
    public void testGlobalRegister_doubleRegistration() throws Exception {
        abstractSerializationService.registerGlobal(new StringBufferSerializer(true));
        abstractSerializationService.registerGlobal(new StringBufferSerializer(true));
    }

    @Test(expected = IllegalStateException.class)
    public void testGlobalRegister_alreadyRegisteredType() throws Exception {
        abstractSerializationService.register(StringBuffer.class, new StringBufferSerializer(true));
        abstractSerializationService.registerGlobal(new TheOtherGlobalSerializer(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSafeRegister_ConstantType() throws Exception {
        abstractSerializationService.safeRegister(Integer.class, new StringBufferSerializer(true));
    }

    @Test(expected = IllegalStateException.class)
    public void testSafeRegister_alreadyRegisteredType() throws Exception {
        abstractSerializationService.safeRegister(StringBuffer.class, new StringBufferSerializer(true));
        abstractSerializationService.safeRegister(StringBuffer.class, new TheOtherGlobalSerializer(true));
    }

    @Test(expected = IllegalStateException.class)
    public void testSafeRegister_alreadyRegisteredTypeId() throws Exception {
        abstractSerializationService.safeRegister(StringBuffer.class, new StringBufferSerializer(true));
        abstractSerializationService.safeRegister(StringBuilder.class, new TheOtherGlobalSerializer(true));
    }

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void testSerializerFor_ServiceInactive() throws Exception {
        abstractSerializationService.dispose();
        abstractSerializationService.serializerFor(new CustomSerializationTest.Foo());
    }

    private class StringBufferSerializer implements StreamSerializer<StringBuffer> {
        int typeId = 100000;
        private boolean fail;

        public StringBufferSerializer(boolean fail) {
            this.fail = fail;
        }

        @Override
        public int getTypeId() {
            return typeId;
        }

        @Override
        public void destroy() {

        }

        @Override
        public void write(ObjectDataOutput out, StringBuffer stringBuffer) throws IOException {
            if (fail) {
                throw new RuntimeException();
            } else {
                out.writeUTF(stringBuffer.toString());
            }
        }

        @Override
        public StringBuffer read(ObjectDataInput in) throws IOException {
            if (fail) {
                throw new RuntimeException();
            } else {
                return new StringBuffer(in.readUTF());
            }
        }
    }

    private class TheOtherGlobalSerializer extends StringBufferSerializer {

        public TheOtherGlobalSerializer(boolean fail) {
            super(fail);
        }
    }

}
