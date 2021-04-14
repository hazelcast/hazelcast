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
import com.hazelcast.sql.SqlBasicTest;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlBasicSlowTest extends SqlBasicTest {

    private static final int[] PAGE_SIZES = {1, 16, 256, 4096};
    private static final int[] DATA_SET_SIZES = {1, 256, 4096};

    @Parameterized.Parameters(name = "cursorBufferSize:{0}, dataSetSize:{1}, serializationMode:{2}, inMemoryFormat:{3}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (int pageSize : PAGE_SIZES) {
            for (int dataSetSize : DATA_SET_SIZES) {
                for (SerializationMode serializationMode : SerializationMode.values()) {
                    for (InMemoryFormat format : new InMemoryFormat[]{InMemoryFormat.OBJECT, InMemoryFormat.BINARY}) {
                        res.add(new Object[]{
                                pageSize,
                                dataSetSize,
                                serializationMode,
                                format
                        });
                    }
                }
            }
        }

        return res;
    }
}
