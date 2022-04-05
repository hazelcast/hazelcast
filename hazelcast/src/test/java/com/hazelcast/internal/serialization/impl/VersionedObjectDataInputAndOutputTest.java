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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VersionedObjectDataInputAndOutputTest {

    private final InternalSerializationService iss = (InternalSerializationService)
            new DefaultSerializationServiceBuilder().setVersion(SerializationServiceV1.VERSION_1).build();

    @Test
    public void testVersionOnInput() {
        ObjectDataInputStream input = new ObjectDataInputStream(new ByteArrayInputStream(new byte[]{}), iss);
        Version version = Versions.V4_0;

        input.setVersion(version);
        assertEquals(version, input.getVersion());
    }

    @Test
    public void testVersionOnOutput() {
        ObjectDataOutputStream output = new ObjectDataOutputStream(new ByteArrayOutputStream(16), iss);
        Version version = Versions.V4_0;

        output.setVersion(version);
        assertEquals(version, output.getVersion());
    }
}
