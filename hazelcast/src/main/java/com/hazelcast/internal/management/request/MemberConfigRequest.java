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

package com.hazelcast.internal.management.request;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.internal.management.ManagementCenterService;

/**
 * Request for fetching member XML configuration.
 */
public class MemberConfigRequest implements ConsoleRequest {

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MEMBER_CONFIG;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) {
        final JsonObject result = new JsonObject();
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator(true);
        Config config = mcs.getHazelcastInstance().getConfig();
        String configXmlString = configXmlGenerator.generate(config);
        result.add("configXmlString", configXmlString);
        root.add("result", result);
    }

    @Override
    public void fromJson(JsonObject json) {
    }
}
