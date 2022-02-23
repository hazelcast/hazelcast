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

package com.hazelcast.internal.util.hashslot.impl;

import com.hazelcast.internal.util.hashslot.SlotAssignmentResult;

/**
 * POJO implementation of the result of a slot assignment invocation.
 */
public class SlotAssignmentResultImpl implements SlotAssignmentResult {
    private long address;
    private boolean isNew;

    @Override
    public long address() {
        return address;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    /**
     * Sets if the last assignment invocation resulted in a new assignment or
     * if there was an existing slot for the given parameters.
     */
    public void setNew(boolean isNew) {
        this.isNew = isNew;
    }

    /**
     * Sets the current slot assignment address.
     */
    public void setAddress(long address) {
        this.address = address;
    }
}
