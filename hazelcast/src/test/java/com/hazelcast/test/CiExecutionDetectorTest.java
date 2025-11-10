/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.CiExecutionDetector.isOnCi;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CiExecutionDetectorTest {

    private static final ILogger LOGGER = Logger.getLogger(CiExecutionDetector.class);

    @Test
    public void testIsOnCi() {
        LOGGER.info("Is TestSuite running on Jenkins? " + isOnCi());
    }

    /**
     * This test is active by default to prevent accidental disabling of CI-specific features
     * (most importantly {@link TestLoggerFactory}).
     * If it is ever needed to execute whole test suite outside CI environment then:
     * <ul>
     *     <li>this test can be excluded</li>
     *     <li>{@code CI=true} env variable can be defined to allow this test to pass</li>
     * </ul>
     */
    @Test
    public void testMustBeOnCi() {
        assertThat(isOnCi()).isTrue();
    }
}
