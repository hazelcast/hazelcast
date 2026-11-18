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

import static com.hazelcast.jet.pulsar.PulsarProperties.stripPrefix;
import static org.assertj.core.api.Assertions.assertThat;

@QuickTest
@ParallelTest
class ConnectionSuppliersTest {

    @ParameterizedTest
    @ValueSource(strings = {"pulsar.client.", "pulsar.admin."})
    void stripPrefix_handlesEmptyOptions(String prefix) {
        assertThat(stripPrefix(Map.of(), prefix)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"pulsar.client.", "pulsar.admin."})
    void stripPrefix_normalizesMixedOptions(String prefix) {
        Map<String, String> options = Map.of(
                prefix + "connectionTimeoutMs", "1234",
                "pulsar.operationTimeoutMs", "5678",
                "numIoThreads", "2");

        assertThat(stripPrefix(options, prefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
                "connectionTimeoutMs", "1234",
                "operationTimeoutMs", "5678",
                "numIoThreads", "2"));
    }

    @Test
    void stripPrefix_order() {
        Map<String, String> options = Map.of(
                "pulsar.client.connectionTimeoutMs", "1234",
                "pulsar.connectionTimeoutMs", "5678",
                "connectionTimeoutMs", "2");

        String additionalPrefix = "pulsar.client.";
        assertThat(stripPrefix(options, additionalPrefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
            "connectionTimeoutMs", "1234"));

        options = Map.of(
            "pulsar.connectionTimeoutMs", "5678",
            "pulsar.client.connectionTimeoutMs", "1234",
            "connectionTimeoutMs", "2");

        assertThat(stripPrefix(options, additionalPrefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
            "connectionTimeoutMs", "1234"));

        options = Map.of(
            "connectionTimeoutMs", "2",
            "pulsar.connectionTimeoutMs", "5678",
            "pulsar.client.connectionTimeoutMs", "1234"
        );
        assertThat(stripPrefix(options, additionalPrefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
            "connectionTimeoutMs", "1234"));

        options = Map.of(
            "connectionTimeoutMs", "2",
            "pulsar.connectionTimeoutMs", "5678"
        );
        assertThat(stripPrefix(options, additionalPrefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
            "connectionTimeoutMs", "5678"));

        options = Map.of(
            "pulsar.connectionTimeoutMs", "5678",
            "connectionTimeoutMs", "2"
        );
        assertThat(stripPrefix(options, additionalPrefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
            "connectionTimeoutMs", "5678"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"pulsar.client.", "pulsar.admin."})
    void stripPrefix_removesOnlyTheLeadingPrefix(String prefix) {
        Map<String, String> options = Map.of(
                prefix + prefix + "option", "first",
                "pulsar.pulsar.option", "second",
                prefix + "pulsar.other", "third",
                "pulsar." + prefix + "other", "fourth",
                "custom." + prefix + "option", "fifth",
                "custom.pulsar.option", "sixth");

        assertThat(stripPrefix(options, prefix)).containsExactlyInAnyOrderEntriesOf(Map.of(
                prefix + "option", "first",
                "pulsar.option", "second",
                "pulsar.other", "third",
                prefix + "other", "fourth",
                "custom." + prefix + "option", "fifth",
                "custom.pulsar.option", "sixth"));
    }

    @Test
    void stripPrefix_preservesOtherConnectionPrefixes() {
        assertThat(stripPrefix(Map.of("admin.requestTimeoutMs", "1000"), "client"))
                .containsExactlyInAnyOrderEntriesOf(Map.of("admin.requestTimeoutMs", "1000"));
        assertThat(stripPrefix(Map.of("client.numIoThreads", "2"), "admin"))
                .containsExactlyInAnyOrderEntriesOf(Map.of("client.numIoThreads", "2"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"pulsar.client.", "pulsar.admin."})
    void stripPrefix_returnsIndependentCopyWithoutMutatingOptions(String prefix) {
        Map<String, String> original = Map.of(prefix + ".numIoThreads", "2", "pulsar.useTls", "true");
        Map<String, String> options = new HashMap<>(original);

        Map<String, String> stripped = stripPrefix(options, prefix);

        assertThat(options).containsExactlyInAnyOrderEntriesOf(original);
        stripped.put("numIoThreads", "3");
        assertThat(options).containsExactlyInAnyOrderEntriesOf(original);
        options.put("pulsar.useTls", "false");
        assertThat(stripped).containsEntry("useTls", "true");
    }
}
