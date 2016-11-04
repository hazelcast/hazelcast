/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec;

public enum JetMessageType {

    JET_INIT(0x1a01),
    JET_SUBMIT(0x1a02),
    JET_EXECUTE(0x1a03),
    JET_INTERRUPT(0x1a04),
    JET_DEPLOY(0x1a06),
    JET_FINISHDEPLOYMENT(0x1a07),
    JET_EVENT(0x1a08),
    JET_GETACCUMULATORS(0x1a09);

    private final int id;

    JetMessageType(int messageType) {
        this.id = messageType;
    }

    public int id() {
        return id;
    }


}


