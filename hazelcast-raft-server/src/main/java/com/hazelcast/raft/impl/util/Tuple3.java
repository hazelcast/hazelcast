package com.hazelcast.raft.impl.util;

/**
 * TODO: Javadoc Pending...
 *
 */
public class Tuple3<X, Y, Z> {

    public static <X, Y, Z> Tuple3<X, Y, Z> of(X element1, Y element2, Z element3) {
        return new Tuple3<X, Y, Z>(element1, element2, element3);
    }

    public final X element1;
    public final Y element2;
    public final Z element3;

    public Tuple3(X element1, Y element2, Z element3) {
        this.element1 = element1;
        this.element2 = element2;
        this.element3 = element3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tuple3<?, ?, ?> tuple3 = (Tuple3<?, ?, ?>) o;

        if (element1 != null ? !element1.equals(tuple3.element1) : tuple3.element1 != null) {
            return false;
        }
        if (element2 != null ? !element2.equals(tuple3.element2) : tuple3.element2 != null) {
            return false;
        }
        return element3 != null ? element3.equals(tuple3.element3) : tuple3.element3 == null;
    }

    @Override
    public int hashCode() {
        int result = element1 != null ? element1.hashCode() : 0;
        result = 31 * result + (element2 != null ? element2.hashCode() : 0);
        result = 31 * result + (element3 != null ? element3.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple3{" + "element1=" + element1 + ", element2=" + element2 + ", element3=" + element3 + '}';
    }
}
