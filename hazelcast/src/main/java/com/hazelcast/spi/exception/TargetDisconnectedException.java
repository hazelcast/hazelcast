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

package com.hazelcast.spi.exception;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;

import static com.hazelcast.util.StringUtil.timeToString;
import static com.hazelcast.util.StringUtil.timeToStringFriendly;
import static java.lang.String.format;

/**
 * A {@link com.hazelcast.spi.exception.RetryableHazelcastException} that indicates that an operation is about to
 * be send to a non existing machine.
 */
public class TargetDisconnectedException extends RetryableHazelcastException {

    public TargetDisconnectedException() {
    }

    public TargetDisconnectedException(Address address) {
        super("Target[" + address + "] disconnected.");
    }

    public TargetDisconnectedException(String message) {
        super(message);
    }

    public TargetDisconnectedException(String message, Throwable cause) {
        super(message, cause);
    }

    public static Exception newTargetDisconnectedExceptionCausedByHeartbeat(Address memberAddress,
                                                                            String connectionString,
                                                                            long lastHeartbeatRequestedMillis,
                                                                            long lastHeartbeatReceivedMillis,
                                                                            long lastReadMillis,
                                                                            Throwable cause) {
        String msg = format(
                "Disconnecting from member %s due to heartbeat problems. "
                        + "Current time: %s. "
                        + "Last heartbeat requested: %s. "
                        + "Last heartbeat received: %s. "
                        + "Last read: %s. "
                        + "Connection %s",
                memberAddress,
                timeToString(System.currentTimeMillis()),
                timeToStringFriendly(lastHeartbeatRequestedMillis),
                timeToStringFriendly(lastHeartbeatReceivedMillis),
                timeToStringFriendly(lastReadMillis),
                connectionString);
        return new TargetDisconnectedException(msg, cause);
    }

    public static Exception newTargetDisconnectedExceptionCausedByMemberLeftEvent(Member member, String connectionString) {
        String msg = format(
                "Closing connection to member %s."
                        + " The client has closed the connection to this member,"
                        + " after receiving a member left event from the cluster. Connection=%s",
                member.getAddress(), connectionString);
        return new TargetDisconnectedException(msg);
    }
}
