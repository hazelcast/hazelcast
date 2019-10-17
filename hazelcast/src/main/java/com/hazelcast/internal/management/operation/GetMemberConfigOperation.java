/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.operation;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

/**
 * Operation to get member config as XML from Management Center.
 */
public class GetMemberConfigOperation extends AbstractLocalOperation {

    private String configXmlString;

    @Override
    public void run() throws Exception {
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator(true);
        Config config = getNodeEngine().getHazelcastInstance().getConfig();
        configXmlString = configXmlGenerator.generate(config);
    }

    @Override
    public Object getResponse() {
        return configXmlString;
    }
}
