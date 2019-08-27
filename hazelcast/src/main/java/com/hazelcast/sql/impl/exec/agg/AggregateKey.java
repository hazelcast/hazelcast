package com.hazelcast.sql.impl.exec.agg;

import java.util.Arrays;

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

    private static class SingleAggregateKey extends AggregateKey {
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
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SingleAggregateKey that = (SingleAggregateKey)o;

            return item != null ? item.equals(that.item) : that.item == null;
        }

        @Override
        public int hashCode() {
            return item != null ? item.hashCode() : 0;
        }
    }

    private static class DualAggregateKey extends AggregateKey {
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
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            DualAggregateKey that = (DualAggregateKey) o;

            if (item1 != null ? !item1.equals(that.item1) : that.item1 != null)
                return false;

            return item2 != null ? item2.equals(that.item2) : that.item2 == null;
        }

        @Override
        public int hashCode() {
            int result = item1 != null ? item1.hashCode() : 0;
            result = 31 * result + (item2 != null ? item2.hashCode() : 0);
            return result;
        }
    }

    private static class MultiAggregateKey extends AggregateKey {
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
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            MultiAggregateKey that = (MultiAggregateKey) o;

            return Arrays.equals(items, that.items);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(items);
        }
    }
}


