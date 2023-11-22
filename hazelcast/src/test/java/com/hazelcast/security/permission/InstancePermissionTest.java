/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.security.permission;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InstancePermissionTest {
    @Test
    void testActions() {
        assertEquals("A B C", new InstantiatableInstancePermission(getClass().getSimpleName(), "A", "B", "C").getActions());
    }

    @NullSource
    @EmptySource
    @ParameterizedTest
    void testInvalidNameThrows(String name) {
        assertThrows(IllegalArgumentException.class, () -> new InstantiatableInstancePermission(name));
    }

    @SuppressWarnings("serial")
    private static class InstantiatableInstancePermission extends InstancePermission {
        protected InstantiatableInstancePermission(String name, String... actions) {
            super(name, actions);
        }

        @Override
        protected int initMask(String[] actions) {
            return Integer.MIN_VALUE;
        }
    }
}
