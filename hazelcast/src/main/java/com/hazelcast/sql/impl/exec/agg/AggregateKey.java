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

package com.hazelcast.sql.impl.exec.agg;

import com.hazelcast.sql.impl.row.Row;

import java.util.Arrays;
import java.util.Objects;

public abstract class AggregateKey {
    public static AggregateKey single(Object item) {
        return new SingleAggregateKey(item);
    }

    public static AggregateKey dual(Object item1, Object item2) {
        return new DualAggregateKey(item1, item2);
    }

    public static AggregateKey multiple(Object... items) {
        return new MultiAggregateKey(items);
    }

    public abstract Object get(int idx);
    public abstract int getCount();
    public abstract boolean matches(Row row);

    private static final class SingleAggregateKey extends AggregateKey {
        private final Object item;

        private SingleAggregateKey(Object item) {
            this.item = item;
        }

        @Override
        public Object get(int idx) {
            assert idx == 0;

            return item;
        }

        @Override
        public int getCount() {
            return 1;
        }

        @Override
        public boolean matches(Row row) {
            return Objects.equals(item, row.get(0));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SingleAggregateKey that = (SingleAggregateKey) o;

            return item != null ? item.equals(that.item) : that.item == null;
        }

        @Override
        public int hashCode() {
            return item != null ? item.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "AggregateKey1{" + item + "}";
        }
    }

    private static final class DualAggregateKey extends AggregateKey {
        private final Object item1;
        private final Object item2;

        private DualAggregateKey(Object item1, Object item2) {
            this.item1 = item1;
            this.item2 = item2;
        }

        @Override
        public Object get(int idx) {
            assert idx == 0 || idx == 1;

            return idx == 0 ? item1 : item2;
        }

        @Override
        public int getCount() {
            return 2;
        }

        @Override
        public boolean matches(Row row) {
            return Objects.equals(item1, row.get(0)) && Objects.equals(item2, row.get(1));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DualAggregateKey that = (DualAggregateKey) o;

            if (item1 != null ? !item1.equals(that.item1) : that.item1 != null) {
                return false;
            }

            return item2 != null ? item2.equals(that.item2) : that.item2 == null;
        }

        @Override
        public int hashCode() {
            int result = item1 != null ? item1.hashCode() : 0;

            result = 31 * result + (item2 != null ? item2.hashCode() : 0);

            return result;
        }

        @Override
        public String toString() {
            return "AggregateKey2{" + item1 + ", " + item2 + "}";
        }
    }

    private static final class MultiAggregateKey extends AggregateKey {
        private final Object[] items;

        private MultiAggregateKey(Object[] items) {
            this.items = items;
        }

        @Override
        public Object get(int idx) {
            assert idx >= 0 && idx < items.length;

            return items[idx];
        }

        @Override
        public int getCount() {
            return items.length;
        }

        @Override
        public boolean matches(Row row) {
            for (int i = 0; i < items.length; i++) {
                if (!Objects.equals(items[i], row.get(i))) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MultiAggregateKey that = (MultiAggregateKey) o;

            return Arrays.equals(items, that.items);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(items);
        }

        @Override
        public String toString() {
            StringBuilder res = new StringBuilder("AggregateKey" + items.length + "{");

            for (int i = 0; i < items.length; i++) {
                res.append(items[i]);

                if (i != 0) {
                    res.append(", ");
                }
            }

            res.append("}");

            return res.toString();
        }
    }
}


