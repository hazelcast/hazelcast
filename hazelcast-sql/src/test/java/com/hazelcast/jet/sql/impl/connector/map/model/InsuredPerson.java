/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.map.model;

import java.io.Serializable;
import java.util.Objects;

@SuppressWarnings("unused")
public class InsuredPerson extends Person implements Serializable {

    private long ssn;

    public InsuredPerson() {
    }

    public InsuredPerson(int id, String name, long ssn) {
        super(id, name);
        this.ssn = ssn;
    }

    public long getSsn() {
        return ssn;
    }

    public void setSsn(long ssn) {
        this.ssn = ssn;
    }

    @Override
    public String toString() {
        return "InsuredPerson{" +
                "ssn='" + ssn + '\'' +
                "} " + super.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        InsuredPerson that = (InsuredPerson) o;
        return Objects.equals(ssn, that.ssn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), ssn);
    }
}
