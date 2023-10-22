/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FieldUtilsTest {

    @Test
    public void test_getEnumConstants() {
        assertThat(FieldUtils.getEnumConstants(EnumLikeClass.class))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "VALUE1", EnumLikeClass.VALUE1,
                        "VALUE2", EnumLikeClass.VALUE2,
                        "VALUE3", EnumLikeClass.VALUE3
                ));
    }

    @SuppressWarnings("unused")
    private static class EnumLikeClass {
        // Enum constants
        public static final EnumLikeClass VALUE1 = new EnumLikeClass(1);
        public static final EnumLikeClass VALUE2 = new EnumLikeClass(2);
        @SuppressWarnings("UnnecessaryModifier")
        public static final transient EnumLikeClass VALUE3 = new EnumLikeClass(3);

        // Invalid access modifier for enum constant
        static final EnumLikeClass VALUE4 = new EnumLikeClass(4);
        protected static final EnumLikeClass VALUE5 = new EnumLikeClass(5);
        private static final EnumLikeClass VALUE6 = new EnumLikeClass(6);

        // Missing modifier for enum constant
        public static EnumLikeClass value7 = new EnumLikeClass(7);
        public final EnumLikeClass value8 = value7;
        public EnumLikeClass value9 = value7;

        private final int value;

        EnumLikeClass(int value) {
            this.value = value;
        }

        int getValue() {
            return value;
        }
    }
}
