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

package com.hazelcast.jet.core.processor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteMapSourceBuilderTest {

    @Test
    void testHasDataSourceConnectionOrClientConfig_NotNull() {
        assertThatThrownBy(() -> Sources.remoteMapBuilder("mapName").build())
                .hasMessage("Either dataConnectionName or clientConfig must be non-null");
    }

    @Test
    void testClientConfig_NotNull() {
        ClientConfig clientConfig = new ClientConfig();
        assertThatCode(() -> Sources.remoteMapBuilder("mapName")
                .clientConfig(clientConfig)
                .build()
        ).doesNotThrowAnyException();
    }

    @Test
    void testDataConnectionName_NotNull() {
        assertThatCode(() -> Sources.remoteMapBuilder("mapName")
                .dataConnectionName("dataConnectionName")
                .build()
        ).doesNotThrowAnyException();
    }

    @Test
    void testNotSerializablePredicate() {
        NonSerializablePredicate predicate = new NonSerializablePredicate();

        var builder = Sources.<Integer, Integer>remoteMapBuilder("mapName");
        assertThatThrownBy(() -> builder.predicate(predicate))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("\"predicate\" must be serializable");
    }

    @Test
    void testNotSerializableProjection() {
        NonSerializableProjection projection = new NonSerializableProjection();

        var builder = Sources.<Integer, Integer>remoteMapBuilder("mapName");
        assertThatThrownBy(() -> builder.projection(projection))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("\"projection\" must be serializable");
    }

    static class NonSerializablePredicate implements Predicate<Integer, Integer> {

        // make class non-serializable
        Object o = new Object();

        @Override
        public boolean apply(Map.Entry<Integer, Integer> t) {
            return !t.getValue().equals(0);
        }
    }

    static class NonSerializableProjection implements Projection<Map.Entry<Integer, Integer>, String> {

        // make class non-serializable
        Object o = new Object();

        @Override
        public String transform(Map.Entry<Integer, Integer> input) {
            return String.valueOf(input.getValue());
        }
    }
}
