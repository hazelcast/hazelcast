/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.spi.AbstractWaitNotifyKey;
import com.hazelcast.util.ValidationUtil;

public class SemaphoreWaitNotifyKey extends AbstractWaitNotifyKey {

    private final String type;

    public SemaphoreWaitNotifyKey(String name, String type) {
        super(SemaphoreService.SERVICE_NAME, name);
        this.type = ValidationUtil.isNotNull(type, "type");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SemaphoreWaitNotifyKey)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        SemaphoreWaitNotifyKey that = (SemaphoreWaitNotifyKey) o;

        if (!type.equals(that.type)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
