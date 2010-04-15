/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.query;

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.Record;
import org.junit.Ignore;

import java.io.Serializable;

import static com.hazelcast.nio.IOUtil.toData;

@Ignore
public class TestUtil {
    public static Record newRecord(long recordId) {
        return new Record(null, 1, null, null, 0, 0, recordId);
    }

    public static Record newRecord(long recordId, Object key, Object value) {
        Record record = newRecord(recordId);
        record.setKey(toData(key));
        record.setValue(toData(value));
        return record;
    }

    @Ignore
    public static class EmptyMapEntry implements MapEntry {
        private long cost;
        private long creationTime;
        private long expirationTime;
        private int hits;
        private long lastAccessTime;
        private long lastUpdateTime;
        private int version;
        private boolean valid;
        private Object key;
        private Object value;
        private long id;

        public EmptyMapEntry(long id) {
            this.id = id;
        }

        public long getCost() {
            return cost;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public long getExpirationTime() {
            return expirationTime;
        }

        public int getHits() {
            return hits;
        }

        public long getLastAccessTime() {
            return lastAccessTime;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public long getVersion() {
            return version;
        }

        public boolean isValid() {
            return valid;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public Object setValue(Object value) {
            Object oldValue = this.value;
            this.value = value;
            return oldValue;
        }

        public void setCost(long cost) {
            this.cost = cost;
        }

        public void setCreationTime(long creationTime) {
            this.creationTime = creationTime;
        }

        public void setExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
        }

        public void setHits(int hits) {
            this.hits = hits;
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public void setLastAccessTime(long lastAccessTime) {
            this.lastAccessTime = lastAccessTime;
        }

        public void setLastUpdateTime(long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public long getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EmptyMapEntry that = (EmptyMapEntry) o;
            if (id != that.id) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }

        @Override
        public String toString() {
            return "EmptyMapEntry{" +
                    "id=" + id +
                    ", expirationTime=" + expirationTime +
                    ", hits=" + hits +
                    ", lastAccessTime=" + lastAccessTime +
                    ", lastUpdateTime=" + lastUpdateTime +
                    ", key=" + key +
                    ", value=" + value +
                    ", valid=" + valid +
                    ", creationTime=" + creationTime +
                    ", cost=" + cost +
                    ", version=" + version +
                    '}';
        }
    }

    @Ignore
    public static class Employee implements Serializable {
        String name;
        int age;
        boolean active;
        double salary;

        public Employee(String name, int age, boolean live, double price) {
            this.name = name;
            this.age = age;
            this.active = live;
            this.salary = price;
        }

        public Employee() {
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        public double getSalary() {
            return salary;
        }

        public boolean isActive() {
            return active;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Employee employee = (Employee) o;
            if (active != employee.active) return false;
            if (age != employee.age) return false;
            if (Double.compare(employee.salary, salary) != 0) return false;
            if (name != null ? !name.equals(employee.name) : employee.name != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = name != null ? name.hashCode() : 0;
            result = 31 * result + age;
            result = 31 * result + (active ? 1 : 0);
            temp = salary != +0.0d ? Double.doubleToLongBits(salary) : 0L;
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("Employee");
            sb.append("{name='").append(name).append('\'');
            sb.append(", age=").append(age);
            sb.append(", active=").append(active);
            sb.append(", salary=").append(salary);
            sb.append('}');
            return sb.toString();
        }
    }
}
