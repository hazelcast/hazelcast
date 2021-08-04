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

package com.hazelcast.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
public class SSLWithoutEnterpriseTest extends HazelcastTestSupport {

    @Test(expected = IllegalStateException.class)
    public void test() {
        Config config = new Config();
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setEnabled(true);

        config.getNetworkConfig().setSSLConfig(sslConfig);

        Hazelcast.newHazelcastInstance(config);
    }
}
