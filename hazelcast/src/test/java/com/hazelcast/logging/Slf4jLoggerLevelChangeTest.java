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

package com.hazelcast.logging;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.IsolatedLoggingRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;

import static com.hazelcast.test.IsolatedLoggingRule.LOGGING_TYPE_SLF4J;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class Slf4jLoggerLevelChangeTest extends HazelcastTestSupport {

    @Rule
    public final IsolatedLoggingRule isolatedLoggingRule = new IsolatedLoggingRule();

    private LoggingServiceImpl loggingService;

    @Before
    public void before() {
        isolatedLoggingRule.setLoggingType(LOGGING_TYPE_SLF4J);
        HazelcastInstance instance = createHazelcastInstance();
        loggingService = (LoggingServiceImpl) instance.getLoggingService();
    }

    @Test(expected = HazelcastException.class)
    public void test() {
        // slf4j is not supported currently
        loggingService.setLevel(Level.FINEST);
    }

}
