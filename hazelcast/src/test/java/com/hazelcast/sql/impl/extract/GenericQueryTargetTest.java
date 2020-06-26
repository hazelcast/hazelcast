/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericQueryTargetTest extends SqlTestSupport {
    @Test
    public void testTargetDescriptor() {
        serializeAndCheck(new GenericQueryTargetDescriptor(), SqlDataSerializerHook.TARGET_DESCRIPTOR_GENERIC);
    }

    @Test
    public void testTarget() {
        checkTarget(createTarget(true));
        checkTarget(createTarget(false));
    }

    private void checkTarget(GenericQueryTarget target) {
        TestObject object = new TestObject(1);

        checkTarget(target, object, object);
        checkTarget(target, object, toData(object));
    }

    private void checkTarget(GenericQueryTarget target, TestObject originalObject, Object object) {
        // Set target.
        target.setTarget(object);

        // Good top-level extractor.
        QueryExtractor targetExtractor = target.createExtractor(null, QueryDataType.OBJECT);
        TestObject extractedObject = (TestObject) targetExtractor.get();
        assertEquals(originalObject.getField(), extractedObject.getField());

        // Bad top-level extractor.
        QueryExtractor badTargetExtractor = target.createExtractor(null, QueryDataType.INT);
        QueryException error = assertThrows(QueryException.class, badTargetExtractor::get);
        assertEquals(SqlErrorCode.DATA_EXCEPTION, error.getCode());
        assertTrue(error.getMessage().startsWith("Failed to extract map entry " + (target.isKey() ? "key" : "value")));

        // Good field executor.
        QueryExtractor fieldExtractor = target.createExtractor("field", QueryDataType.OBJECT);
        int extractedField = (Integer) fieldExtractor.get();
        assertEquals(originalObject.getField(), extractedField);

        // Bad field extractor (type).
        QueryExtractor badFieldTypeExtractor = target.createExtractor("field", QueryDataType.DATE);
        error = assertThrows(QueryException.class, badFieldTypeExtractor::get);
        assertEquals(SqlErrorCode.DATA_EXCEPTION, error.getCode());
        assertTrue(error.getMessage().startsWith("Failed to extract map entry " + (target.isKey() ? "key" : "value") + " field"));

        // Bad field extractor (name).
        QueryExtractor badFieldNameExtractor = target.createExtractor("field2", QueryDataType.INT);
        error = assertThrows(QueryException.class, badFieldNameExtractor::get);
        assertEquals(SqlErrorCode.DATA_EXCEPTION, error.getCode());
        assertTrue(error.getMessage().startsWith("Failed to extract map entry " + (target.isKey() ? "key" : "value") + " field"));
    }

    private static Data toData(TestObject object) {
        return new DefaultSerializationServiceBuilder().build().toData(object);
    }

    private static GenericQueryTarget createTarget(boolean key) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        Extractors extractors = Extractors.newBuilder(ss).build();

        GenericQueryTargetDescriptor descriptor = new GenericQueryTargetDescriptor();

        GenericQueryTarget target = (GenericQueryTarget) descriptor.create(ss, extractors, key);

        assertEquals(key, target.isKey());

        return target;
    }

    @SuppressWarnings("unused")
    private static class TestObject implements DataSerializable {

        private int field;

        private TestObject() {
            // No-op.
        }

        private TestObject(int field) {
            this.field = field;
        }

        private int getField() {
            return field;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(field);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            field = in.readInt();
        }
    }
}
