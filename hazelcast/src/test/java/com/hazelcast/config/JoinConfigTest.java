/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class JoinConfigTest {

    @Test
    public void joinConfigTest() {
        assertOk(false, false, false);
        assertOk(true, false, false);
        assertOk(false, true, false);
        assertOk(false, false, true);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void joinConfigTestWhenTwoJoinMethodEnabled() {
        assertOk(true, true, false);
    }

    public static void assertOk(boolean tcp, boolean multicast, boolean aws) {
        JoinConfig config = new JoinConfig();
        config.getMulticastConfig().setEnabled(multicast);
        config.getTcpIpConfig().setEnabled(tcp);
        config.getAwsConfig().setEnabled(aws);

        config.verify();
    }
}
