/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.submitjob.clientside.execute;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.impl.submitjob.clientside.SubmitJobTargetMemberFinder;

import java.nio.file.Path;
import java.util.UUID;

/**
 * Holds calculated parameters required for direct job execution
 */
public class JobExecuteCall {

    private UUID sessionId;

    private UUID memberUuid;

    private String jarPath;


    public void initializeJobExecuteCall(HazelcastClientInstanceImpl client, Path jarPath) {
        this.sessionId = UuidUtil.newSecureUUID();
        this.jarPath = jarPath.toString();

        // Find the destination member
        SubmitJobTargetMemberFinder submitJobTargetMemberFinder = new SubmitJobTargetMemberFinder();
        this.memberUuid = submitJobTargetMemberFinder.getRandomMemberId(client);
    }

    public UUID getSessionId() {
        return sessionId;
    }

    public UUID getMemberUuid() {
        return memberUuid;
    }

    public String getJarPath() {
        return jarPath;
    }

    public String getSha256HexOfJar() {
        return "";
    }
}
