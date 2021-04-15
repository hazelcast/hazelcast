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

package com.hazelcast.sql_slow;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.sql.SqlBasicTest.SerializationMode;
import com.hazelcast.sql.SqlOrderByTest;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.sql.SqlBasicTest.SerializationMode.IDENTIFIED_DATA_SERIALIZABLE;
import static com.hazelcast.sql.SqlBasicTest.SerializationMode.SERIALIZABLE;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlOrderBySlowTest extends SqlOrderByTest {
    @Parameterized.Parameters(name = "serializationMode:{0}, inMemoryFormat:{1}, membersCount:{2}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (int membersCount : Collections.singletonList(3)) {
            for (SerializationMode serializationMode : Arrays.asList(SERIALIZABLE, IDENTIFIED_DATA_SERIALIZABLE)) {
                for (InMemoryFormat format : new InMemoryFormat[]{InMemoryFormat.OBJECT, InMemoryFormat.BINARY}) {
                    res.add(new Object[]{
                            serializationMode,
                            format,
                            membersCount
                    });
                }
            }
        }

        return res;
    }
}
