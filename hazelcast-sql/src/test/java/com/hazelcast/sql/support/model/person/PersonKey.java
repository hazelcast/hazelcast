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

package com.hazelcast.sql.support.model.person;

import java.io.Serializable;

public class PersonKey implements Serializable {
    private static final long serialVersionUID = -5213552088903336752L;

    private long id;
    private long deptId;

    public PersonKey() {
        // No-op.
    }

    public PersonKey(long id, long deptId) {
        this.id = id;
        this.deptId = deptId;
    }

    public long getId() {
        return id;
    }

    public long getDeptId() {
        return deptId;
    }
}
