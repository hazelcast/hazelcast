/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.pulsar;

import com.hazelcast.test.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.pulsar.PulsarProperties.filterOptions;
import static org.assertj.core.api.Assertions.assertThat;

@QuickTest
@ParallelTest
class ConnectionSuppliersTest {

    @ParameterizedTest
    @ValueSource(strings = {"pulsar.client.", "pulsar.admin."})
    void filterOptions_handlesEmptyOptions(String prefix) {
        assertThat(filterOptions(Map.of(), prefix)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"pulsar.client", "pulsar.admin"})
    void filterOptions_normalizesMixedOptions(String prefix) {
        Map<String, String> options = Map.of(
                prefix + ".connectionTimeoutMs", "1234",
                "pulsar.operationTimeoutMs", "5678",
                "numIoThreads", "2");

        assertThat(filterOptions(options, prefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
                "connectionTimeoutMs", "1234"));
    }

    @Test
    void filterOptions_order() {
        Map<String, String> options = Map.of(
                "pulsar.client.connectionTimeoutMs", "1234",
                "pulsar.connectionTimeoutMs", "5678",
                "connectionTimeoutMs", "2");

        String additionalPrefix = "pulsar.client";
        assertThat(filterOptions(options, additionalPrefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
            "connectionTimeoutMs", "1234"));

        options = Map.of(
            "pulsar.connectionTimeoutMs", "5678",
            "pulsar.client.connectionTimeoutMs", "1234",
            "connectionTimeoutMs", "2");

        assertThat(filterOptions(options, additionalPrefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
            "connectionTimeoutMs", "1234"));

        options = Map.of(
            "connectionTimeoutMs", "2",
            "pulsar.connectionTimeoutMs", "5678",
            "pulsar.client.connectionTimeoutMs", "1234"
        );
        assertThat(filterOptions(options, additionalPrefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
            "connectionTimeoutMs", "1234"));

        options = Map.of(
            "connectionTimeoutMs", "2",
            "pulsar.connectionTimeoutMs", "5678"
        );
        assertThat(filterOptions(options, additionalPrefix)).isEmpty();

        options = Map.of(
            "pulsar.client.connectionTimeoutMs", "5678",
            "connectionTimeoutMs", "2"
        );
        assertThat(filterOptions(options, additionalPrefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
            "connectionTimeoutMs", "5678"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"pulsar.client", "pulsar.admin"})
    void filterOptions_removesOnlyTheLeadingPrefix(String prefix) {
        Map<String, String> options = Map.of(
                prefix + "." + prefix + ".option", "first",
                "pulsar.pulsar.option", "second",
                prefix + ".pulsar.other", "third",
                "pulsar." + prefix + ".other", "fourth",
                "custom." + prefix + ".option", "fifth",
                "custom.pulsar.option", "sixth");

        assertThat(filterOptions(options, prefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
                prefix + ".option", "first",
                "pulsar.other", "third"));
    }

    @Test
    void filterOptions_preservesOtherConnectionPrefixes() {
        assertThat(filterOptions(Map.of("pulsar.client.requestTimeoutMs", "1000"), "pulsar.client"))
                .containsExactlyInAnyOrderEntriesOf(Map.of("requestTimeoutMs", "1000"));
        assertThat(filterOptions(Map.of("pulsar.admin.numIoThreads", "2"), "pulsar.admin"))
                .containsExactlyInAnyOrderEntriesOf(Map.of("numIoThreads", "2"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"pulsar.client", "pulsar.admin"})
    void filterOptions_returnsIndependentCopyWithoutMutatingOptions(String prefix) {
        Map<String, String> original = Map.of(prefix + ".numIoThreads", "2", "pulsar.useTls", "true");
        Map<String, String> options = new HashMap<>(original);

        Map<String, String> filtered = filterOptions(options, prefix);

        assertThat(options).containsExactlyInAnyOrderEntriesOf(original);
        filtered.put("numIoThreads", "3");
        assertThat(options).containsExactlyInAnyOrderEntriesOf(original);
    }
}
