/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.util.IOUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Base64;

import static com.hazelcast.test.Accessors.getSerializationService;
import static java.lang.ClassLoader.getSystemResourceAsStream;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SearchableExpressionCompatibilityTest extends HazelcastTestSupport {
    public static final String DAG_BASE64 = "dag_ids_5.1.3.base64";
    public static final String EXECUTION_PLAN_BASE64 = "exec_plan_ids_5.1.3.base64";

    @Test
    public void when_deserializingPreviousPatchVersion() throws IOException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance = factory.newHazelcastInstance();
        InternalSerializationService serializationService = getSerializationService(instance);

        for (String file : Arrays.asList(DAG_BASE64, EXECUTION_PLAN_BASE64)) {
            try (
                    InputStream inputStream = getSystemResourceAsStream(file);
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            ) {
                assert inputStream != null;

                IOUtil.copyStream(inputStream, bytes);
                Data data = new HeapData(Base64.getDecoder().decode(bytes.toString()));
                Object object = serializationService.toObject(data);

                assert object instanceof DAG || object instanceof ExecutionPlan;
            }
        }
        factory.terminateAll();
    }
}
