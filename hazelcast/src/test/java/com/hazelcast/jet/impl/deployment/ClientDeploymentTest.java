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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.config.Config;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonList;

@RunWith(HazelcastSerialClassRunner.class)
@Ignore
public class ClientDeploymentTest extends AbstractDeploymentTest {

    @Rule
    public final Timeout timeoutRule = Timeout.seconds(360);

    @BeforeClass
    public static void beforeClass() {
        Config config = new Config();
        FilteringClassLoader filteringClassLoader = new FilteringClassLoader(singletonList("deployment"), null);
        config.setClassLoader(filteringClassLoader);
        //TODO the config is created but not passed to the initialize method
        initializeWithClient(2, null, null);
    }

    @Override
    protected JetInstance getJetInstance() {
        return client();
    }
}
