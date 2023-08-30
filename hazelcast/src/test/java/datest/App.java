package datest;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.concurrent.TimeUnit;

public class App {

    public static void main(final String args[]) {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();

        final IMap<String, TestPojo> map = instance.getMap("test-map");
        map.addInterceptor(new TestInterceptor());
        map.addLocalEntryListener(new TestListener());

        final TestPojo testPojo = new TestPojo("Version1");
        System.out.println("Adding new entry -> " + testPojo);
        map.put("key", testPojo);
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println(
                "Performing executeOnKey - update existing entry using entry processor value1->value2, expect interceptor to change field1 from value2 to value3");
        map.executeOnKey("key", new TestProcessor(new TestPojo("Version2")));

    }
}
