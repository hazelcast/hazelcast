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

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/*
 * When executed with HazelcastParallelClassRunner, this test creates
 * a massive amount of threads (peaks of 1000 threads and 800 daemon
 * threads). As comparison the BasicMapTest creates about 700 threads
 * and 25 daemon threads. This regularly results in test failures when
 * multiple PR builders run in parallel due to a resource starvation.
 *
 * Countermeasures are to remove the ParallelJVMTest
 * annotation or to use the HazelcastSerialClassRunner.
 *
 * Without ParallelJVMTest we'll add the whole test duration
 * to the PR builder time (about 25 seconds) and still create
 * the resource usage peak, which may have a negative impact
 * on parallel PR builder runs on the same host machine.
 *
 * With HazelcastSerialClassRunner the test takes over 3 minutes, but
 * with a maximum of 200 threads and 160 daemon threads. This should
 * have less impact on other tests and the total duration of the PR
 * build (since the test will still be executed in parallel to others).
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class GracefulShutdownSlowTest extends GracefulShutdownTest {

    @Override
    protected Config getConfig() {
        return regularInstanceConfig();
    }
}
