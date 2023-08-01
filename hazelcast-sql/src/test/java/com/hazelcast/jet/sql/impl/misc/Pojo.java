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

package com.hazelcast.jet.sql.impl.misc;

import java.io.Serializable;

public class Pojo implements Serializable {
    private Integer f0;
    private Integer f1;
    private Integer f2;

    public Pojo() {
    }

    public Pojo(Integer f0, Integer f1, Integer f2) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
    }

    public Integer getF0() {
        return f0;
    }

    public void setF0(Integer f0) {
        this.f0 = f0;
    }

    public Integer getF1() {
        return f1;
    }

    public void setF1(Integer f1) {
        this.f1 = f1;
    }

    public Integer getF2() {
        return f2;
    }

    public void setF2(Integer f2) {
        this.f2 = f2;
    }
}
