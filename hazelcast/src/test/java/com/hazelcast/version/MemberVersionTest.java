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

package com.hazelcast.version;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static com.hazelcast.version.MemberVersion.MAJOR_MINOR_VERSION_COMPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuickTest
public class MemberVersionTest {

    private MemberVersion version = MemberVersion.of(3, 8, 0);
    private MemberVersion versionSameAttributes = MemberVersion.of(3, 8, 0);
    private MemberVersion versionOtherMajor = MemberVersion.of(4, 8, 0);
    private MemberVersion versionOtherMinor = MemberVersion.of(3, 9, 0);
    private MemberVersion versionOtherPatch = MemberVersion.of(3, 8, 1);

    @Test
    public void testIsUnknown() {
        assertTrue(MemberVersion.UNKNOWN.isUnknown());
    }

    @Test
    public void testVersionOf_whenVersionIsUnknown() {
        assertEquals(MemberVersion.UNKNOWN, MemberVersion.of(0, 0, 0));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', useHeadersInDisplayName = true, textBlock = """
                Version String    | Major | Minor | Patch
                3.8-SNAPSHOT      | 3     | 8     | 0
                3.8-beta-2        | 3     | 8     | 0
                3.8.1-beta-1      | 3     | 8     | 1
                3.8.1-beta-2      | 3     | 8     | 1
                3.8.1-RC1         | 3     | 8     | 1
                3.8.2             | 3     | 8     | 2
            """)
    public void testVersionOf(String version, int major, int minor, int patch) {
        MemberVersion parsed = MemberVersion.of(version);

        assertEquals(MemberVersion.of(major, minor, patch), parsed);
        assertFalse(parsed.isUnknown());
    }

    @Test
    public void test_constituents() {
        MemberVersion expected = MemberVersion.of(3, 8, 2);

        assertEquals(3, expected.getMajor());
        assertEquals(8, expected.getMinor());
        assertEquals(2, expected.getPatch());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "0.0.0")
    public void testVersionOf_unknown(String version) {
        assertEquals(MemberVersion.UNKNOWN, MemberVersion.of(version));
    }

    @Test
    public void testEquals() {
        assertEquals(version, version);
        assertEquals(version, versionSameAttributes);

        assertNotEquals(null, version);
        assertNotEquals(new Object(), version);

        assertNotEquals(version, versionOtherMajor);
        assertNotEquals(version, versionOtherMinor);
        assertNotEquals(version, versionOtherPatch);
    }

    @Test
    public void testHashCode() {
        assertThat(version).hasSameHashCodeAs(version);
        assertThat(version).hasSameHashCodeAs(versionSameAttributes);

        assumeDifferentHashCodes();
        assertThat(version).doesNotHaveSameHashCodeAs(versionOtherMajor);
        assertThat(version).doesNotHaveSameHashCodeAs(versionOtherMinor);
        assertThat(version).doesNotHaveSameHashCodeAs(versionOtherPatch);
    }

    @Test
    public void testCompareTo() {
        assertThat(version).isEqualByComparingTo(version);
        assertThat(version).isLessThan(versionOtherMinor);
        assertThat(version).isLessThan(versionOtherPatch);

        assertThat(versionOtherMinor).isGreaterThan(version);
        assertThat(versionOtherMinor).isGreaterThan(versionOtherPatch);
    }

    @Test
    public void testMajorMinorVersionComparator() {
        assertEquals(0, MAJOR_MINOR_VERSION_COMPARATOR.compare(version, versionOtherPatch));
        assertThat(MAJOR_MINOR_VERSION_COMPARATOR.compare(versionOtherMinor, version)).isPositive();
        assertThat(MAJOR_MINOR_VERSION_COMPARATOR.compare(version, versionOtherMinor)).isNegative();
        assertThat(MAJOR_MINOR_VERSION_COMPARATOR.compare(versionOtherMinor, versionOtherPatch)).isPositive();
        assertThat(MAJOR_MINOR_VERSION_COMPARATOR.compare(versionOtherPatch, versionOtherMinor)).isNegative();
    }

    @Test
    public void testAsClusterVersion() {
        Version clusterVersion = MemberVersion.of(3, 8, 2).asVersion();

        assertEquals(3, clusterVersion.getMajor());
        assertEquals(8, clusterVersion.getMinor());
    }

    @Test
    public void testAsSerializationVersion() {
        Version version = MemberVersion.of(4, 0, 2).asVersion();

        assertEquals(Versions.V4_0, version);
    }

    @Test
    public void testEmpty() {
        MemberVersion version = new MemberVersion();

        assertEquals(0, version.getMajor());
        assertEquals(0, version.getMinor());
        assertEquals(0, version.getPatch());
    }

    @Test
    public void testSerialization() {
        MemberVersion given = MemberVersion.of(3, 9, 1);
        SerializationServiceV1 ss = new DefaultSerializationServiceBuilder().setVersion(SerializationServiceV1.VERSION_1).build();
        MemberVersion deserialized = ss.toObject(ss.toData(given));

        assertEquals(deserialized, given);
    }

    @Test
    public void toStringTest() {
        String version = "3.8.2";
        assertEquals(version, MemberVersion.of(version).toString());
    }
}
