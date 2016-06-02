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

package com.hazelcast.jet.internal.api.application;

import com.hazelcast.jet.internal.impl.hazelcast.JetPacket;
import com.hazelcast.nio.Address;

/**
 * Exception of some JET application
 */
public class ApplicationException extends RuntimeException {
    private final String initiatorAddress;
    private final JetPacket wrongPacket;
    private final Object reason;

    public ApplicationException(Object reason, Address initiator) {
        this.reason = reason;
        this.initiatorAddress = initiator != null ? initiator.toString() : "";
        this.wrongPacket = null;
    }

    public ApplicationException(Address initiator) {
        this.initiatorAddress = initiator != null ? initiator.toString() : "";
        this.wrongPacket = null;
        this.reason = null;
    }

    public ApplicationException(Address initiator, JetPacket wrongPacket) {
        this.initiatorAddress = initiator != null ? initiator.toString() : "";
        this.wrongPacket = wrongPacket;
        this.reason = null;
    }

    @Override
    public Throwable getCause() {
        if ((this.reason != null) && (this.reason instanceof Throwable)) {
            return (Throwable) this.reason;
        }

        return null;
    }

    @Override
    public String getMessage() {
        return toString();
    }

    public String toString() {
        if (this.initiatorAddress != null) {
            String error = "Application was invalidated by member with address: " + initiatorAddress;

            if (this.wrongPacket == null) {
                return error;
            } else {
                return error + " wrongPacket=" + wrongPacket.toString();
            }
        } else if (this.reason != null) {
            return "Application was invalidated because of : " + this.reason.toString();
        } else {
            return "Application was invalidated for unknown reason";
        }
    }
}
