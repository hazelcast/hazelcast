/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.record;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@Tag("com.hazelcast.test.annotation.QuickTest")
@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
class LazyCollectionTest {
    private static LazyCollection<?, ?> collection;

    @BeforeAll
    public static void setUp() {
        collection = new LazyCollection<>(mock(), new InternalReplicatedMapStorage<>());
    }

    private static Stream<Named<Executable>> unsupportedOperations() {
        return Stream.of(Named.of("contains", () -> collection.contains(null)),
                Named.of("containsAll", () -> collection.containsAll(Collections.emptyList())),
                Named.of("add", () -> collection.add(null)),
                Named.of("addAll", () -> collection.addAll(Collections.emptyList())),
                Named.of("remove", () -> collection.remove(null)),
                Named.of("removeAll", () -> collection.removeAll(Collections.emptyList())),
                Named.of("retainAll", () -> collection.retainAll(Collections.emptyList())),
                Named.of("clear", () -> collection.clear()));
    }

    @ParameterizedTest
    @MethodSource("unsupportedOperations")
    void testUnsupportedOperations(Executable operation) {
        assertThrows(UnsupportedOperationException.class, operation);
    }
}
