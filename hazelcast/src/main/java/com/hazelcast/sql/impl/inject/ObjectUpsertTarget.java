/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.inject;

public class ObjectUpsertTarget implements UpsertTarget {

    ObjectUpsertTarget() {
    }

    @Override
    public Target get() {
        // TODO: reuse ???
        return new ObjectTarget();
    }

    @Override
    public UpsertInjector createInjector(String path) {
        return (target, value) -> ((ObjectTarget) target).set(value);
    }

    private static class ObjectTarget implements Target {

        private Object value;

        private void set(Object value) {
            this.value = value;
        }

        @Override
        public Object conclude() {
            return value;
        }
    }
}
