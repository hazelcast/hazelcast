/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.mapstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class JDBCParameters {
    private int idPos;

    private Object[] params;

    void setIdPos(int idPos) {
        this.idPos = idPos;
    }

    Object[] getParams() {
        return params;
    }

    void setParams(Object[] params) {
        this.params = params;
    }

    void shiftParametersForUpdate() {
        // Move the id parameter to last position and shift everything else down
        List<Object> paramsList = new ArrayList<>(Arrays.asList(params));
        Object idValue = paramsList.remove(idPos);
        paramsList.add(idValue);
        params = paramsList.toArray(new Object[0]);
    }
}
