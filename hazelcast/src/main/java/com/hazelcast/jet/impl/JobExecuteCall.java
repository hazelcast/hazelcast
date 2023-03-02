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

package com.hazelcast.jet.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;

import java.nio.file.Path;
import java.util.UUID;

public class JobExecuteCall {

    private UUID sessionId;

    private UUID memberUuid;

    private String jarPath;

    private String sha256HexOfJar = "";


    public void initializeJobExecuteCall(HazelcastClientInstanceImpl client, Path jarPath) {

        // Create a special session id
        this.sessionId = new UUID(0, 0);
        this.jarPath = jarPath.toString();

        // Find the destination member
        SubmitJobTargetMemberFinder submitJobTargetMemberFinder = new SubmitJobTargetMemberFinder();
        this.memberUuid = submitJobTargetMemberFinder.getRandomMemberId(client);
    }

    UUID getSessionId() {
        return sessionId;
    }

    // This method is public for testing purposes.
    public UUID getMemberUuid() {
        return memberUuid;
    }

    public String getJarPath() {
        return jarPath;
    }

    String getSha256HexOfJar() {
        return sha256HexOfJar;
    }

}
