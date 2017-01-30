/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.logging.ILogger;

import java.io.IOException;

import static com.hazelcast.util.JsonUtil.getString;

/**
 * Request coming from Management Center to trigger a force start the node during hot-restart.
 */
public class ForceStartNodeRequest implements ConsoleRequest {

    /**
     * Result sent within response when force start is triggered successfully
     */
    public static final String SUCCESS_RESULT = "SUCCESS";

    /**
     * Result sent within response when force start triggering failed
     */
    public static final String FAILED_RESULT = "FAILED";

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_FORCE_START_NODE;
    }

    @Override
    public Object readResponse(JsonObject in) throws IOException {
        return getString(in, "result", "FAIL");
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject out) throws Exception {
        String resultString;
        HazelcastInstanceImpl instance = mcs.getHazelcastInstance();
        try {
            resultString = instance.node.getNodeExtension().getInternalHotRestartService().triggerForceStart()
                    ? SUCCESS_RESULT : FAILED_RESULT;
        } catch (Exception e) {
            ILogger logger = instance.node.getLogger(getClass());
            logger.warning("Problem on force start: ", e);
            resultString = e.getMessage();
        }

        JsonObject result = new JsonObject().add("result", resultString);
        out.add("result", result);
    }

    @Override
    public JsonObject toJson() {
        return new JsonObject();
    }

    @Override
    public void fromJson(JsonObject json) {
    }
}
