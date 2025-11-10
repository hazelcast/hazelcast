/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.TypedDataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractSerializationServiceTest {

    private AbstractSerializationService abstractSerializationService;

    @Before
    public void setup() {
        abstractSerializationService = newAbstractSerializationService();
    }

    protected AbstractSerializationService newAbstractSerializationService() {
        return new DefaultSerializationServiceBuilder()
                .setVersion(InternalSerializationService.VERSION_1)
                .setNotActiveExceptionSupplier(HazelcastInstanceNotActiveException::new)
                .build();
    }

    @Test
    public void toBytes_withPadding() {
        String payload = "somepayload";
        int padding = 10;

        byte[] unpadded = abstractSerializationService.toBytes(payload, 0, true);
        byte[] padded = abstractSerializationService.toBytes(payload, 10, true);

        // make sure the size is expected
        assertEquals(unpadded.length + padding, padded.length);

        // check if the actual content is the same
        for (int k = 0; k < unpadded.length; k++) {
            assertEquals(unpadded[k], padded[k + padding]);
        }
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
    public void testToBytesHandleThrowable() {
        abstractSerializationService.register(StringBuffer.class, new StringBufferSerializer(true));
        abstractSerializationService.toBytes(new StringBuffer());
    }

    @Test
    public void testToObject_ServiceInactive() {
        abstractSerializationService.register(StringBuffer.class, new StringBufferSerializer(false));
        Data data = abstractSerializationService.toData(new StringBuffer());
        abstractSerializationService.dispose();
        Assert.assertThrows(HazelcastInstanceNotActiveException.class, () -> abstractSerializationService.toObject(data));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testWriteObject_serializerFail() {
        abstractSerializationService.register(StringBuffer.class, new StringBufferSerializer(true));
        BufferObjectDataOutput out = abstractSerializationService.createObjectDataOutput();
        abstractSerializationService.writeObject(out, new StringBuffer());
    }

    @Test
    public void testReadObject_ServiceInactive() {
        abstractSerializationService.register(StringBuffer.class, new StringBufferSerializer(false));
        Data data = abstractSerializationService.toData(new StringBuffer());
        abstractSerializationService.dispose();

        BufferObjectDataInput in = abstractSerializationService.createObjectDataInput(data);
        in.position(HeapData.TYPE_OFFSET);
        Assert.assertThrows(HazelcastInstanceNotActiveException.class, () -> abstractSerializationService.readObject(in));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegister_nullType() {
        abstractSerializationService.register(null, new StringBufferSerializer(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegister_typeIdNegative() {
        StringBufferSerializer serializer = new StringBufferSerializer(true);
        serializer.typeId = -10000;
        abstractSerializationService.register(StringBuffer.class, serializer);
    }

    @Test(expected = IllegalStateException.class)
    public void testGlobalRegister_doubleRegistration() {
        abstractSerializationService.registerGlobal(new StringBufferSerializer(true));
        abstractSerializationService.registerGlobal(new StringBufferSerializer(true));
    }

    @Test(expected = IllegalStateException.class)
    public void testGlobalRegister_alreadyRegisteredType() {
        abstractSerializationService.register(StringBuffer.class, new StringBufferSerializer(true));
        abstractSerializationService.registerGlobal(new TheOtherGlobalSerializer(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSafeRegister_ConstantType() {
        abstractSerializationService.safeRegister(Integer.class, new StringBufferSerializer(true));
    }

    @Test(expected = IllegalStateException.class)
    public void testSafeRegister_alreadyRegisteredType() {
        abstractSerializationService.safeRegister(StringBuffer.class, new StringBufferSerializer(true));
        abstractSerializationService.safeRegister(StringBuffer.class, new TheOtherGlobalSerializer(true));
    }

    @Test(expected = IllegalStateException.class)
    public void testSafeRegister_alreadyRegisteredTypeId() {
        abstractSerializationService.safeRegister(StringBuffer.class, new StringBufferSerializer(true));
        abstractSerializationService.safeRegister(StringBuilder.class, new TheOtherGlobalSerializer(true));
    }

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void testSerializerFor_ServiceInactive() {
        abstractSerializationService.dispose();
        abstractSerializationService.serializerFor(new CustomSerializationTest.Foo(), false);
    }

    @Test
    public void testDeserializationForSpecificType() {
        BaseClass baseObject = new BaseClass(5, "abc");
        ExtendedClass extendedObject = new ExtendedClass(baseObject, 378);
        Data extendedData = abstractSerializationService.toData(extendedObject);

        Object deserializedObject = abstractSerializationService.toObject(extendedData);
        assertEquals(extendedObject, deserializedObject);

        deserializedObject = abstractSerializationService.toObject(extendedObject, BaseClass.class);
        assertEquals(baseObject, deserializedObject);
    }

    @Test
    public void testTypedSerialization() {
        BaseClass baseObject = new BaseClass();
        Data data = abstractSerializationService.toData(baseObject);
        Object deserializedObject = abstractSerializationService.toObject(data);
        assertEquals(baseObject, deserializedObject);

        TypedBaseClass typedBaseObject = new TypedBaseClass(baseObject);
        Data typedData = abstractSerializationService.toData(typedBaseObject);

        deserializedObject = abstractSerializationService.toObject(typedData);
        assertEquals(BaseClass.class, deserializedObject.getClass());
        assertEquals(baseObject, deserializedObject);

        deserializedObject = abstractSerializationService.toObject(typedData, TypedBaseClass.class);
        assertEquals(typedBaseObject, deserializedObject);
    }

    public static class TypedBaseClass implements DataSerializable, TypedDataSerializable {
        private final BaseClass innerObj;

        public TypedBaseClass() {
            innerObj = new BaseClass();
        }

        public TypedBaseClass(BaseClass innerObj) {
            this.innerObj = innerObj;
        }

        @Override
        public Class getClassType() {
            return BaseClass.class;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            innerObj.writeData(out);
            out.writeInt(innerObj.intField);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            innerObj.readData(in);
            innerObj.intField = in.readInt();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (null == obj) {
                return false;
            }
            if (null == obj || !(getClass().isAssignableFrom(obj.getClass()))) {
                return false;
            }

            TypedBaseClass rhs = (TypedBaseClass) obj;
            if (null == innerObj && null != rhs.innerObj || null != innerObj && null == rhs.innerObj) {
                return false;
            }
            if (null == innerObj && null == rhs.innerObj) {
                return true;
            }
            return innerObj.equals(rhs.innerObj);
        }

        @Override
        public int hashCode() {
            return innerObj != null ? innerObj.hashCode() : 0;
        }
    }

    public static class BaseClass implements DataSerializable {

        private int intField;
        private String stringField;

        public BaseClass() {
        }

        public BaseClass(BaseClass rhs) {
            this.intField = rhs.intField;
            this.stringField = rhs.stringField;
        }

        public BaseClass(int intField, String stringField) {
            this.intField = intField;
            this.stringField = stringField;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(intField);
            out.writeString(stringField);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            intField = in.readInt();
            stringField = in.readString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || !getClass().isAssignableFrom(o.getClass())) {
                return false;
            }

            BaseClass baseClass = (BaseClass) o;
            if (intField != baseClass.intField) {
                return false;
            }
            return stringField != null ? stringField.equals(baseClass.stringField) : baseClass.stringField == null;
        }

        @Override
        public int hashCode() {
            int result = intField;
            result = 31 * result + (stringField != null ? stringField.hashCode() : 0);
            return result;
        }
    }

    public static class ExtendedClass extends BaseClass {

        private long longField;

        public ExtendedClass() {
        }

        public ExtendedClass(BaseClass baseObject, long longField) {
            super(baseObject);
            this.longField = longField;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            super.writeData(out);
            out.writeLong(longField);

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            super.readData(in);
            longField = in.readLong();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || !getClass().isAssignableFrom(o.getClass())) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }

            ExtendedClass that = (ExtendedClass) o;
            return longField == that.longField;

        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (int) (longField ^ (longField >>> 32));
            return result;
        }
    }

    private class StringBufferSerializer implements StreamSerializer<StringBuffer> {

        int typeId = 100000;
        private boolean fail;

        StringBufferSerializer(boolean fail) {
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
                out.writeString(stringBuffer.toString());
            }
        }

        @Override
        public StringBuffer read(ObjectDataInput in) throws IOException {
            if (fail) {
                throw new RuntimeException();
            } else {
                return new StringBuffer(in.readString());
            }
        }
    }

    private class TheOtherGlobalSerializer extends StringBufferSerializer {

        TheOtherGlobalSerializer(boolean fail) {
            super(fail);
        }
    }
}
