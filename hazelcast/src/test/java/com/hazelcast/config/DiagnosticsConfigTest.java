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
package com.hazelcast.config;

import com.hazelcast.internal.diagnostics.DiagnosticsConfig;
import com.hazelcast.internal.diagnostics.DiagnosticsOutputType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertThrows;
import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiagnosticsConfigTest {

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(DiagnosticsConfig.class)
                .suppress(Warning.STRICT_INHERITANCE)
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }

    @Test
    public void testDiagnosticsConfigCanBeSerialized() {
        DiagnosticsConfig config = new DiagnosticsConfig()
                .setLogDirectory("/src/user")
                .setIncludeEpochTime(true)
                .setFileNamePrefix("my-logs")
                .setOutputType(DiagnosticsOutputType.STDOUT)
                .setMaxRolledFileSizeInMB(99)
                .setMaxRolledFileCount(89)
                .setAutoOffDurationInMinutes(5)
                .setEnabled(true);
        config.getPluginProperties().put("prop1", "prop1");

        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        Data serializedData = ss.toData(config);
        DiagnosticsConfig deserializedConfig = ss.toObject(serializedData);

        assertEquals(config.isEnabled(), deserializedConfig.isEnabled());
        assertEquals(config.getPluginProperties(), deserializedConfig.getPluginProperties());
        assertEquals(config.getMaxRolledFileCount(), deserializedConfig.getMaxRolledFileCount());
        assertEquals(config.getMaxRolledFileSizeInMB(), deserializedConfig.getMaxRolledFileSizeInMB(), 0);
        assertEquals(config.getLogDirectory(), deserializedConfig.getLogDirectory());
        assertEquals(config.getFileNamePrefix(), deserializedConfig.getFileNamePrefix());
        assertEquals(config.getOutputType(), deserializedConfig.getOutputType());
        assertEquals(config.isIncludeEpochTime(), deserializedConfig.isIncludeEpochTime());
        assertEquals(config.getAutoOffDurationInMinutes(), deserializedConfig.getAutoOffDurationInMinutes());
    }

    @Test
    public void TestDiagnosticsConfigSetNull() {
        DiagnosticsConfig config = new DiagnosticsConfig();

        assertThrows(IllegalArgumentException.class, () -> config.setMaxRolledFileCount(-1));
        assertThrows(IllegalArgumentException.class, () -> config.setMaxRolledFileSizeInMB(-1));
        assertThrows(IllegalArgumentException.class, () -> config.setMaxRolledFileSizeInMB(0));
        assertThrows(IllegalArgumentException.class, () -> config.setMaxRolledFileCount(0));
        assertThrows(IllegalArgumentException.class, () -> config.setLogDirectory(""));
        assertThrows(IllegalStateException.class, () -> config.setAutoOffDurationInMinutes(0));
        config.setFileNamePrefix("");
        config.setFileNamePrefix(null);
        assertThrows(NullPointerException.class, () -> config.setOutputType(null));
    }

    @Test
    public void testNullAssigmentThrowsException() {
        DiagnosticsConfig config = new DiagnosticsConfig();
        assertThrows(IllegalArgumentException.class, () -> config.setLogDirectory(null));
        assertThrows(NullPointerException.class, () -> config.setOutputType(null));
    }

}
