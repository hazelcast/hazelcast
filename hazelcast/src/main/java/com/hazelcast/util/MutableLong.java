package com.hazelcast.util;

/**
 * Mutable long which can be used for counting purposes.
 * <p/>
 * This class is not thread-safe.
 */
public class MutableLong {

    //CHECKSTYLE:OFF
    public long value;
    //CHECKSTYLE:OFF

    public static MutableLong valueOf(long value) {
        MutableLong instance = new MutableLong();
        instance.value = value;
        return instance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MutableLong that = (MutableLong) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return "MutableLong{" +
                "value=" + value +
                '}';
    }
}
