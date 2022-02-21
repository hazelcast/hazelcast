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

package example.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

public class ExternalizableEmployeeDTO implements Externalizable {

    private int id;
    private String name;

    // This is set to true on writeExternal and readExternal methods.
    private boolean usedExternalizableSerialization = false;

    public ExternalizableEmployeeDTO() {
    }

    public ExternalizableEmployeeDTO(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public boolean usedExternalizableSerialization() {
        return usedExternalizableSerialization;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExternalizableEmployeeDTO that = (ExternalizableEmployeeDTO) o;
        return id == that.id && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        usedExternalizableSerialization = true;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        usedExternalizableSerialization = true;
    }
}
