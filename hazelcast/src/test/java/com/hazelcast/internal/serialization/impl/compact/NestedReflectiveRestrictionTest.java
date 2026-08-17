/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Objects;

import static com.hazelcast.test.HazelcastTestSupport.addToCompactSerializationAllowList;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NestedReflectiveRestrictionTest {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private HazelcastInstance member;
    private HazelcastInstance client;

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Before
    public void before() {
        member = factory.newHazelcastInstance(createConfig());
        client = factory.newHazelcastClient(createClientConfig());
    }

    private ClientConfig createClientConfig() {
        ClientConfig config = new ClientConfig();
        addToCompactSerializationAllowList(config.getSerializationConfig(),
                TestDto.class, TestRecordDto.class, IntWrapper.class, LongWrapper.class, MyInterface.class);
        return config;
    }

    private Config createConfig() {
        Config config = HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics();
        addToCompactSerializationAllowList(config.getSerializationConfig(),
                TestDto.class, TestRecordDto.class, LongWrapper.class, MyInterface.class);
        return config;
    }

    @Test
    public void testWritingRestrictedImplementation() {
        InternalSerializationService ss = Accessors.getSerializationService(member);
        TestDto obj = new TestDto(new IntWrapper(1), "abc");
        assertThatThrownBy(() -> ss.toData(obj)).rootCause()
                                                .isInstanceOf(ReflectiveCompactSerializationUnsupportedException.class);
    }

    @Test
    public void testWritingUnrestrictedImplementation() {
        InternalSerializationService ss = Accessors.getSerializationService(member);
        TestDto obj = new TestDto(new LongWrapper(1), "abc");
        Data serialized = ss.toData(obj);
        TestDto deserialized = ss.toObject(serialized);
        assertThat(deserialized).isEqualTo(obj);
    }

    @Test
    public void testClientWritingRestrictedImplementationAndMemberReading() {
        testClientWritingRestrictedImplementationAndMemberReadingImpl(
                entry(new TestDto(new IntWrapper(6), "xyz"), intWrapperGenericRecord(TestDto.class, "xyz", 6)),
                new TestDto(new LongWrapper(7), "abc"));
    }

    @Test
    public void testClientWritingRestrictedImplementationAndMemberReadingRecord() {
        testClientWritingRestrictedImplementationAndMemberReadingImpl(
                entry(new TestRecordDto(new IntWrapper(6), "xyz"), intWrapperGenericRecord(TestRecordDto.class, "xyz", 6)),
                new TestRecordDto(new LongWrapper(7), "abc"));
    }

    private <T> void testClientWritingRestrictedImplementationAndMemberReadingImpl(Map.Entry<T, GenericRecord> restrictedPair,
                                                                                   T unrestrictedObject) {
        IMap<Integer, Object> clientMap = client.getMap("map");
        IMap<Integer, Object> memberMap = member.getMap("map");

        clientMap.set(0, unrestrictedObject);
        assertThat(memberMap.get(0)).isEqualTo(unrestrictedObject);
        assertThat(clientMap.get(0)).isEqualTo(unrestrictedObject);

        clientMap.set(1, restrictedPair.getKey());
        assertThat(clientMap.get(1)).isEqualTo(restrictedPair.getKey());

        assertThat(memberMap.get(0)).isEqualTo(unrestrictedObject);
        // We can read the restricted object to GenericRecord
        assertThat(memberMap.get(1)).isEqualTo(restrictedPair.getValue());

        // We can still read unrestricted objects as expected
        assertThat(memberMap.get(0)).isEqualTo(unrestrictedObject);

        assertThat(clientMap.get(0)).isEqualTo(unrestrictedObject);
        assertThat(clientMap.get(1)).isEqualTo(restrictedPair.getKey());
    }

    private GenericRecord intWrapperGenericRecord(Class<?> topClazz, String otherField, int n) {
        GenericRecord subRecord = GenericRecordBuilder.compact(IntWrapper.class.getName()).setInt32("n", n).build();
        return GenericRecordBuilder.compact(topClazz.getName()).setString("otherField", otherField)
                                   .setGenericRecord("wrapped", subRecord).build();
    }

    public interface MyInterface {
    }

    public static class IntWrapper implements MyInterface {
        private int n;

        public IntWrapper(int n) {
            this.n = n;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IntWrapper that = (IntWrapper) o;
            return n == that.n;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(n);
        }
    }

    public static class LongWrapper implements MyInterface {
        private long n;

        public LongWrapper(long n) {
            this.n = n;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LongWrapper that = (LongWrapper) o;
            return n == that.n;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(n);
        }
    }

    public static class TestDto {
        private final MyInterface wrapped;
        private final String otherField;

        public TestDto(MyInterface wrapped, String otherField) {
            this.wrapped = wrapped;
            this.otherField = otherField;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestDto testDto = (TestDto) o;
            return Objects.equals(wrapped, testDto.wrapped) && Objects.equals(otherField, testDto.otherField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(wrapped, otherField);
        }
    }

    public record TestRecordDto(MyInterface wrapped, String otherField) {
    }
}
