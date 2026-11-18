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

package com.hazelcast.internal.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Known-answer vectors for {@link HashUtil}'s Murmur hash functions, shared by
 * {@code HashUtilMurmurReferenceVectorTest} (this module) and
 * {@code NativeMemoryDataUtilHashReferenceVectorTest} (hazelcast-enterprise, via this module's test-jar).
 * <p>
 * x86_32 values: externally verified against the third-party Python {@code mmh3} package (the real,
 * standard MurmurHash3_x86_32 algorithm) at Hazelcast's seed ({@code 0x01000193}).
 * <p>
 * x64_64 values: Hazelcast's own non-standard variant -- it is NOT the real MurmurHash3_x64_128
 * ({@code mmh3} does not apply here), so no external ground truth exists. Instead, these values were
 * produced by the actual released 5.7.0 jar and confirmed identical to what this (post-CTT-1470) code
 * produces, i.e. a cross-version regression check rather than an independently-verified correctness check.
 */
public final class MurmurHashReferenceVectors {

    private MurmurHashReferenceVectors() {
    }

    /** One known-answer case: a payload and its expected Murmur hash under both variants. */
    public static final class TestValue {
        public final String label;
        public final byte[] payload;
        public final int expectedMurmur32;
        public final long expectedMurmur64;

        TestValue(String label, byte[] payload, int expectedMurmur32, long expectedMurmur64) {
            this.label = label;
            this.payload = payload;
            this.expectedMurmur32 = expectedMurmur32;
            this.expectedMurmur64 = expectedMurmur64;
        }

        @Override
        public String toString() {
            return label;
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] range(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) {
            b[i] = (byte) i;
        }
        return b;
    }

    private static byte[] fill(int n, byte v) {
        byte[] b = new byte[n];
        Arrays.fill(b, v);
        return b;
    }

    public static List<TestValue> vectors() {
        return List.of(
                new TestValue("empty", bytes(""), -1585187909, 9168145165656307917L),
                new TestValue("\"a\"", bytes("a"), -1686100800, 1026673004001772663L),
                new TestValue("\"ab\"", bytes("ab"), 312914265, -5905744999454465287L),
                new TestValue("\"abc\"", bytes("abc"), -2068121803, -6748478652752918386L),
                new TestValue("\"abcdefgh\" (8 bytes, exact x64 tail boundary)",
                        bytes("abcdefgh"), 133362683, -2018815746082553857L),
                new TestValue("\"abcdefghijklmno\" (15 bytes)",
                        bytes("abcdefghijklmno"), -1131701738, -1623292850470368038L),
                new TestValue("\"abcdefghijklmnop\" (16 bytes, exact x64 block boundary)",
                        bytes("abcdefghijklmnop"), 1363628687, 3018113321603575994L),
                new TestValue("range(16)", range(16), 2035805824, 2462466348024822273L),
                new TestValue("range(32)", range(32), -1225219695, 3911938888777733807L),
                new TestValue("16 zero bytes", fill(16, (byte) 0x00), 1436982361, -4679700680931564323L),
                new TestValue("16 0xFF bytes", fill(16, (byte) 0xFF), 429560521, 7024920433113256089L),
                new TestValue("1025 'B' bytes",
                        fill(1025, (byte) 0x42), -692264970, 4705165923623402297L),

                new TestValue("1024 range", range(1024), 412266234, -4710838938739735705L),
                new TestValue("1024 + 2 tail", range(1026), 633412120, -5519209241562547912L),
                new TestValue("1024 + 3 tail", range(1027), 18182672, -5992109409843886011L),
                new TestValue("1024 + 4 tail", range(1028), -1118836702, 8156077649319230054L),
                new TestValue("1024 + 5 tail", range(1029), -238574464, -518084866078643838L),
                new TestValue("1024 + 6 tail", range(1030), 309063627, 1490814945946836032L),
                new TestValue("1024 + 7 tail", range(1031), 1482433422, 1418771691149630404L),
                new TestValue("1024 + 8 tail", range(1032), -1874168791, 7007393741578921969L),
                new TestValue("1024 + 9 tail", range(1033), -366547065, 4279723702802455100L),
                new TestValue("1024 + 10 tail", range(1034), -242278805, -2948663476907330748L),
                new TestValue("1024 + 11 tail", range(1035), -1005426383, -1520604125522423818L),
                new TestValue("1024 + 12 tail", range(1036), 173098228, -4582205177904216278L),
                new TestValue("1024 + 13 tail", range(1037), 1292359558, -6192516678781458447L),
                new TestValue("1024 + 14 tail", range(1038), -1177962097, 3206586254799741293L),
                new TestValue("1024 + 15 tail", range(1039), 1317518819, -8375413947477053215L)
        );
    }
}
