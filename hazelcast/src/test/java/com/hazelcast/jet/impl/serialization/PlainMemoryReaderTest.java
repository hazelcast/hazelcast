/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.serialization;

import org.junit.Test;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

public class PlainMemoryReaderTest {

    @Test
    public void when_littleEndian_then_correctValuesAreRead() {
        // Given
        MemoryReader reader = new PlainMemoryReader(LITTLE_ENDIAN);
        byte[] bytes = new byte[]{
                1, 0, 0, 0,
                2, 0, 0, 0, 0, 0, 0, 0
        };

        // When
        // Then
        assertThat(reader.readInt(bytes, 0)).isEqualTo(1);
        assertThat(reader.readLong(bytes, Integer.BYTES)).isEqualTo(2);
    }

    @Test
    public void when_bigEndian_then_correctValuesAreRead() {
        // Given
        MemoryReader reader = new PlainMemoryReader(BIG_ENDIAN);
        byte[] bytes = new byte[]{
                0, 0, 0, 1,
                0, 0, 0, 0, 0, 0, 0, 2
        };

        // When
        // Then
        assertThat(reader.readInt(bytes, 0)).isEqualTo(1);
        assertThat(reader.readLong(bytes, Integer.BYTES)).isEqualTo(2);
    }
}
