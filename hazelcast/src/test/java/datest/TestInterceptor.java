package datest;

import com.hazelcast.map.MapInterceptor;

public class TestInterceptor implements MapInterceptor {
    private static final long serialVersionUID = 4971835785800511499L;

    @Override
    public Object interceptGet(final Object value) {
        return value;
    }

    @Override
    public void afterGet(final Object value) {
    }

    @Override
    public Object interceptPut(final Object oldValue, final Object newValue) {
        System.out.println("Interceptor put called old: " + oldValue + " new: " + newValue);
        if (newValue.equals(new TestPojo("Version2"))) {
            System.out.println("Interceptor put called old: " + oldValue + ", new: " + newValue + ", replaced with 'Version3'");
            return new TestPojo("Version3");
        }
        return newValue != null ? newValue : oldValue;
    }

    @Override
    public void afterPut(final Object value) {

    }

    @Override
    public Object interceptRemove(final Object removedValue) {
        return null;
    }

    @Override
    public void afterRemove(final Object oldValue) {

    }
}
