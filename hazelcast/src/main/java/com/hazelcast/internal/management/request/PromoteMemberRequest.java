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

package com.hazelcast.internal.management.request;

import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.json.JsonObject;

public class PromoteMemberRequest implements AsyncConsoleRequest {
    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_PROMOTE_MEMBER;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject out) throws Exception {
        JsonObject result = new JsonObject();

        try {
            mcs.getHazelcastInstance().getCluster().promoteLocalLiteMember();
            result.add("success", true);
        } catch (IllegalStateException e) {
            result.add("success", false);
            result.add("message", e.getMessage());
        }

        out.add("result", result);
    }

    @Override
    public void fromJson(JsonObject json) {
    }
}
