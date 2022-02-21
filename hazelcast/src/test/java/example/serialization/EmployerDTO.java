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

import java.util.Arrays;
import java.util.Objects;

public class EmployerDTO {

    private String name;
    private int zcode;
    private HiringStatus hiringStatus;
    private long[] ids;
    private EmployeeDTO singleEmployee;
    private EmployeeDTO[] otherEmployees;

    public EmployerDTO() {
    }

    public EmployerDTO(String name, int zcode, HiringStatus hiringStatus, long[] ids, EmployeeDTO singleEmployee, EmployeeDTO[] otherEmployees) {
        this.name = name;
        this.zcode = zcode;
        this.hiringStatus = hiringStatus;
        this.ids = ids;
        this.singleEmployee = singleEmployee;
        this.otherEmployees = otherEmployees;
    }

    public String getName() {
        return name;
    }

    public int getZcode() {
        return zcode;
    }

    public HiringStatus getHiringStatus() {
        return hiringStatus;
    }

    public EmployeeDTO getSingleEmployee() {
        return singleEmployee;
    }

    public long[] getIds() {
        return ids;
    }

    public EmployeeDTO[] getOtherEmployees() {
        return otherEmployees;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EmployerDTO that = (EmployerDTO) o;

        if (zcode != that.zcode) {
            return false;
        }
        if (hiringStatus != that.hiringStatus) {
            return false;
        }
        if (!Objects.equals(name, that.name)) {
            return false;
        }
        if (!Arrays.equals(ids, that.ids)) {
            return false;
        }
        if (!Objects.equals(singleEmployee, that.singleEmployee)) {
            return false;
        }
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(otherEmployees, that.otherEmployees);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + zcode;
        result = 31 * result + (hiringStatus == null ? 0 : hiringStatus.hashCode());
        result = 31 * result + Arrays.hashCode(ids);
        result = 31 * result + (singleEmployee != null ? singleEmployee.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(otherEmployees);
        return result;
    }

    @Override
    public String toString() {
        return "EmployerDTO{"
                + "name='" + name + '\''
                + ", zcode=" + zcode
                + ", hiringStatus=" + hiringStatus
                + ", ids=" + Arrays.toString(ids)
                + ", singleEmployee=" + singleEmployee
                + ", otherEmployees=" + Arrays.toString(otherEmployees)
                + '}';
    }
}
