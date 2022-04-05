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

package com.hazelcast.cp.exception;

import com.hazelcast.cp.CPGroupId;

/**
 * A {@code CPSubsystemException} which is thrown when a request is sent to
 * a destroyed CP group.
 */
public class CPGroupDestroyedException extends CPSubsystemException {

    private static final long serialVersionUID = -5363753263443789491L;

    private final CPGroupId groupId;

    public CPGroupDestroyedException() {
        this(null);
    }

    public CPGroupDestroyedException(CPGroupId groupId) {
        super(String.valueOf(groupId), null);
        this.groupId = groupId;
    }

    private CPGroupDestroyedException(CPGroupId groupId, Throwable cause) {
        super(String.valueOf(groupId), cause, null);
        this.groupId = groupId;
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

    @Override
    public CPGroupDestroyedException wrap() {
        return new CPGroupDestroyedException(groupId, this);
    }
}
