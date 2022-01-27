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

package com.hazelcast.jet.sql_slow;

import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.impl.connector.map.index.SqlIndexResolutionTest;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@SuppressWarnings("FieldCanBeLocal")
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlSlowIndexResolutionTest extends SqlIndexResolutionTest {

    @BeforeClass
    public static void beforeClass() {
        initialize(3, null);
    }

    @Parameterized.Parameters(name = "indexType:{0}, composite:{1}, type1:{2}, type2:{3}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (IndexType indexType : Arrays.asList(IndexType.SORTED, IndexType.HASH)) {
            for (boolean composite : Arrays.asList(true, false)) {
                for (ExpressionType<?> t1 : nonBaseTypes()) {
                    for (ExpressionType<?> t2 : nonBaseTypes()) {
                        res.add(new Object[]{indexType, composite, t1, t2});
                    }
                }
            }
        }

        return res;
    }
}
