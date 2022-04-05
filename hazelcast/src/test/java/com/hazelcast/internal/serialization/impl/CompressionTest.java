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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class CompressionTest {

    public static class SampleSerializable implements Serializable {

        private int x;

        public SampleSerializable() {
        }

        public SampleSerializable(int x) {
            this.x = x;
        }

        public int getX() {
            return x;
        }

        public void setX(int x) {
            this.x = x;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SampleSerializable that = (SampleSerializable) o;

            return x == that.x;
        }

        @Override
        public int hashCode() {
            return x;
        }
    }

    public static class SampleExternalizable implements Externalizable {

        private int x;

        public SampleExternalizable() {
        }

        public SampleExternalizable(int x) {
            this.x = x;
        }

        public int getX() {
            return x;
        }

        public void setX(int x) {
            this.x = x;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SampleExternalizable that = (SampleExternalizable) o;

            return x == that.x;
        }

        @Override
        public int hashCode() {
            return x;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(x);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            x = in.readInt();
        }
    }


    @Test
    public void testCompression_serializable() {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder = new DefaultSerializationServiceBuilder();
        SerializationService ss = defaultSerializationServiceBuilder.setEnableCompression(true).build();

        SampleSerializable expected = new SampleSerializable(5);
        Data data = ss.toData(expected);
        SampleSerializable result = ss.toObject(data);

        assertEquals(expected, result);
    }

    @Test
    public void testCompression_serializable_withArrayList() {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder = new DefaultSerializationServiceBuilder();
        SerializationService ss = defaultSerializationServiceBuilder.setEnableCompression(true).build();

        ArrayList<SampleSerializable> expected = new ArrayList<SampleSerializable>();
        for (int i = 0; i < 10; i++) {
            expected.add(new SampleSerializable(i));
        }
        Data data = ss.toData(expected);
        ArrayList<SampleSerializable> result = ss.toObject(data);

        assertEquals(expected, result);
    }

    @Test
    public void testCompression_externalizable() {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder = new DefaultSerializationServiceBuilder();
        SerializationService ss = defaultSerializationServiceBuilder.setEnableCompression(true).build();

        SampleExternalizable expected = new SampleExternalizable(5);
        Data data = ss.toData(expected);
        SampleExternalizable result = ss.toObject(data);

        assertEquals(expected, result);
    }

    @Test
    public void testCompression_externalizable_withArrayList() {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder = new DefaultSerializationServiceBuilder();
        SerializationService ss = defaultSerializationServiceBuilder.setEnableCompression(true).build();

        ArrayList<SampleExternalizable> expected = new ArrayList<SampleExternalizable>();
        for (int i = 0; i < 10; i++) {
            expected.add(new SampleExternalizable(i));
        }
        Data data = ss.toData(expected);
        ArrayList<SampleExternalizable> result = ss.toObject(data);

        assertEquals(expected, result);
    }

}
