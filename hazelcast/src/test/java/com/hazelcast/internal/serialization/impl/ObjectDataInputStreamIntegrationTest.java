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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ObjectDataInputStreamIntegrationTest
        extends AbstractDataStreamIntegrationTest<ObjectDataOutputStream, ObjectDataInputStream> {

    private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    @Override
    protected byte[] getWrittenBytes() {
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    protected ObjectDataOutputStream getDataOutput(InternalSerializationService serializationService) {
        return new ObjectDataOutputStream(byteArrayOutputStream, serializationService);
    }

    @Override
    protected ObjectDataInputStream getDataInputFromOutput() {
        return new ObjectDataInputStream(new ByteArrayInputStream(getWrittenBytes()), serializationService);
    }
}
