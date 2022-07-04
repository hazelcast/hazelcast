/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.test.compatibility.SamplingSerializationService;

@SuppressWarnings("WeakerAccess")
public final class TestEnvironment {

    public static final String HAZELCAST_TEST_USE_NETWORK = "hazelcast.test.use.network";
    public static final String EXECUTE_COMPATIBILITY_TESTS = "hazelcast.test.compatibility";
    /**
     * If defined, should indicate a filename prefix to an existing directory where files will be created to dump
     * samples of objects which were serialized/deserialized during test suite execution. Two files per JVM are
     * created, an index and the serialized samples file. The property's value is used as prefix for the filename,
     * followed by a random UUID to avoid clashes.
     * For example {@code -Dhazelcast.test.sample.serialized.objects=/home/hz/tmp/objects-} will create files
     * {@code /home/hz/tmp/objects-UUID.index} and {@code /home/hz/tmp/objects-UUID.samples}.
     *
     * @see SamplingSerializationService
     */
    public static final String SAMPLE_SERIALIZED_OBJECTS = "hazelcast.test.sample.serialized.objects";

    private TestEnvironment() {
    }

    public static boolean isMockNetwork() {
        return !Boolean.getBoolean(HAZELCAST_TEST_USE_NETWORK);
    }

    /**
     * @return {@code true} when compatibility tests are to be executed on a mixed version cluster
     */
    public static boolean isRunningCompatibilityTest() {
        return Boolean.getBoolean(EXECUTE_COMPATIBILITY_TESTS);
    }

    public static boolean isRecordingSerializedClassNames() {
        return System.getProperty(SAMPLE_SERIALIZED_OBJECTS) != null;
    }

    public static String getSerializedClassNamesPath() {
        return System.getProperty(SAMPLE_SERIALIZED_OBJECTS);
    }

    public static boolean isSolaris() {
        return System.getProperty("os.name").startsWith("SunOS");
    }
}
