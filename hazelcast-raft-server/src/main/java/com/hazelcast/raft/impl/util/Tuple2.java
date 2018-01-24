package com.hazelcast.raft.impl.util;

/**
 * TODO: Javadoc Pending...
 *
 */
public class Tuple2<X, Y> {

    public static <X, Y> Tuple2<X, Y> of(X element1, Y element2) {
        return new Tuple2<X, Y>(element1, element2);
    }

    public final X element1;
    public final Y element2;

    public Tuple2(X element1, Y element2) {
        this.element1 = element1;
        this.element2 = element2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;

        if (element1 != null ? !element1.equals(tuple2.element1) : tuple2.element1 != null) {
            return false;
        }
        return element2 != null ? element2.equals(tuple2.element2) : tuple2.element2 == null;
    }

    @Override
    public int hashCode() {
        int result = element1 != null ? element1.hashCode() : 0;
        result = 31 * result + (element2 != null ? element2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple2{" + "element1=" + element1 + ", element2=" + element2 + '}';
    }
}
