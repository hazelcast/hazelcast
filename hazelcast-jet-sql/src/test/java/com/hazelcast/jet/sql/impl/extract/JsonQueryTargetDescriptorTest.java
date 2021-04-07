/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.extract;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.extract.QueryTarget;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class JsonQueryTargetDescriptorTest {

    private static final InternalSerializationService SERIALIZATION_SERVICE =
            new DefaultSerializationServiceBuilder().build();

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_create(boolean key) {
        Extractors extractors = Extractors.newBuilder(SERIALIZATION_SERVICE).build();

        JsonQueryTargetDescriptor descriptor = JsonQueryTargetDescriptor.INSTANCE;

        // when
        QueryTarget target = descriptor.create(SERIALIZATION_SERVICE, extractors, key);

        // then
        assertThat(target).isInstanceOf(JsonQueryTarget.class);
    }

    @Test
    public void test_serialization() {
        JsonQueryTargetDescriptor original = JsonQueryTargetDescriptor.INSTANCE;

        // when
        JsonQueryTargetDescriptor serialized =
                SERIALIZATION_SERVICE.toObject(SERIALIZATION_SERVICE.toData(original));

        // then
        assertThat(serialized).isEqualToComparingFieldByField(original);
    }
}
