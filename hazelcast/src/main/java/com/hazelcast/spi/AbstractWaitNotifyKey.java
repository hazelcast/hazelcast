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

package com.hazelcast.spi;

/**
 * Abstract implementation of the WaitNotifyKey.
 */
public abstract class AbstractWaitNotifyKey implements WaitNotifyKey {

    private final String service;
    private final String objectName;

    protected AbstractWaitNotifyKey(String service, String objectName) {
        this.service = service;
        this.objectName = objectName;
    }

    @Override
    public final String getServiceName() {
        return service;
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractWaitNotifyKey that = (AbstractWaitNotifyKey) o;

        if (objectName != null ? !objectName.equals(that.objectName) : that.objectName != null) {
            return false;
        }
        if (service != null ? !service.equals(that.service) : that.service != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = service != null ? service.hashCode() : 0;
        result = 31 * result + (objectName != null ? objectName.hashCode() : 0);
        return result;
    }
}
