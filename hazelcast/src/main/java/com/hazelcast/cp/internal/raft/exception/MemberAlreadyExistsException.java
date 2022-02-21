/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.exception;

import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;

/**
 * A {@code CPSubsystemException} which is thrown when a member, which is
 * requested to be added to a CP group, is already member of that group.
 * Handled internally.
 */
public class MemberAlreadyExistsException extends CPSubsystemException {

    private static final long serialVersionUID = -4895279676261366826L;

    public MemberAlreadyExistsException(RaftEndpoint member) {
        super("Member already exists: " + member, null);
    }

    private MemberAlreadyExistsException(String message, Throwable cause) {
        super(message, cause, null);
    }

    @Override
    public MemberAlreadyExistsException wrap() {
        return new MemberAlreadyExistsException(getMessage(), this);
    }
}
