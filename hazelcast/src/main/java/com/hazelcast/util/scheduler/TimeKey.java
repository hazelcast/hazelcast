package com.hazelcast.util.scheduler;

/**
 * @author enesakar 9/4/13
 *
 * This object is first needed for having different key objects that are created for the same key updated in different times so should be persisted consequently.
 * So using ScheduleType.FOR_EACH, all the updates will be scheduled seperately.
 */
public class TimeKey {

    private Object key;
    private long time;

    public TimeKey(Object key, long time) {
        this.key = key;
        this.time = time;
    }

    public Object getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeKey that = (TimeKey) o;

        if (time != that.time) return false;
        if (key != null ? !key.equals(that.key) : that.key != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (int) (time ^ (time >>> 32));
        return result;
    }
}
