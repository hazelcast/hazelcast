package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;

public class Main extends HazelcastTestSupport {

    public static void main(String[] args) {
        Object response = null;

        System.out.println(!(response instanceof Throwable));
//
//
//        setLoggingLog4j();
//
//        Config config = new Config();
//        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS, "5000");
//        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
//        System.out.println("starting");
//
//        IAtomicReference ref = hz.getAtomicReference("foo");
//        ref.alter(new IFunction() {
//            @Override
//            public Object apply(Object input) {
//                try {
//                    Thread.sleep(10000000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                return null;
//            }
//        });
    }
}
