/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.spi.AbstractWaitNotifyKey;

public class ScheduledExecutorWaitNotifyKey
        extends AbstractWaitNotifyKey {

    private final String urn;

    public ScheduledExecutorWaitNotifyKey(String objectName, String urn) {
        super(DistributedScheduledExecutorService.SERVICE_NAME, objectName);
        this.urn = urn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ScheduledExecutorWaitNotifyKey)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ScheduledExecutorWaitNotifyKey that = (ScheduledExecutorWaitNotifyKey) o;
        return urn.equals(that.urn);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + urn.hashCode();
        return result;
    }
}
