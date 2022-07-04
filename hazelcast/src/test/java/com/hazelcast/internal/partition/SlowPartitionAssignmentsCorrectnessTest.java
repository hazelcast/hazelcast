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

package com.hazelcast.internal.partition;

import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class})
// related issue https://github.com/hazelcast/hazelcast/issues/5444
public class SlowPartitionAssignmentsCorrectnessTest extends AbstractPartitionAssignmentsCorrectnessTest {

    @Parameters(name = "backups:{0},nodes:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1, 2},
                {1, InternalPartition.MAX_REPLICA_COUNT},
                {2, 3},
                {2, InternalPartition.MAX_REPLICA_COUNT},
                {3, 4},
                {3, InternalPartition.MAX_REPLICA_COUNT},
        });
    }
}
