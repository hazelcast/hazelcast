/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.transaction;

import java.io.Serializable;

public class DummyObject implements Serializable {

    private final Long id;
    private final String string;

    public DummyObject(long id, String string) {
        this.id = id;
        this.string = string;
    }

    public Long getId() {
        return id;
    }

    public String getString() {
        return string;
    }

    @Override
    public int hashCode() {
        if (id == null) {
            return super.hashCode();
        }

        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DummyObject)) {
            return false;
        }

        DummyObject other = (DummyObject) obj;
        if (id == other.id) {
            return true;
        }

        return id != null && id.equals(other.id);
    }

}
