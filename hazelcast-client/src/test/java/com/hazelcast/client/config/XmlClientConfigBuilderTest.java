/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

/**
 * This class tests the usage of {@link XmlClientConfigBuilder}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class XmlClientConfigBuilderTest {


    @Test
    public void readClientExecutorPoolSize() {
        String xml =
                "<hazelcast-client>\n" +
                        "<executor-pool-size>18</executor-pool-size>" +
                        "</hazelcast-client>";
        final ClientConfig clientConfig = buildConfig(xml);
        Assert.assertEquals(18, clientConfig.getExecutorPoolSize());
    }

    private ClientConfig buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(bis);
        return configBuilder.build();
    }

}
