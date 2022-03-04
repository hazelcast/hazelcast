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

package com.hazelcast.transaction.impl;

public class TransactionalObjectKey {

    private final String serviceName;
    private final String name;

    public TransactionalObjectKey(String serviceName, String name) {
        this.serviceName = serviceName;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TransactionalObjectKey)) {
            return false;
        }

        TransactionalObjectKey that = (TransactionalObjectKey) o;

        if (!name.equals(that.name)) {
            return false;
        }
        if (!serviceName.equals(that.serviceName)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = serviceName.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }
}
