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

package com.hazelcast.jet.impl.serialization;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MemoryDataInputTest {

    @Test
    public void when_NotEnoughBytesToRead_then_ThrowsException() {
        // Given
        MemoryDataInput input = new MemoryDataInput(new byte[]{});

        // When
        // Then
        assertThatThrownBy(input::readInt).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(input::readLong).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void when_EnoughBytes_then_ReadsCorrectValues() {
        // Given
        MemoryDataInput input = new MemoryDataInput(new byte[]{
                1, 0, 0, 0,
                2, 0, 0, 0, 0, 0, 0, 0
        });

        // When
        // Then
        assertThat(input.readInt()).isEqualTo(1);
        assertThat(input.readLong()).isEqualTo(2);
    }
}
