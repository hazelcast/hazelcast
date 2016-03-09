package com.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Semaphore;

import static java.util.concurrent.TimeUnit.MINUTES;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IMapPutAsyncStressTest extends HazelcastTestSupport {

    @Test
    public void test() throws InterruptedException {
        setLoggingLog4j();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IMap map = client.getMap("f");
        final Semaphore semaphore = new Semaphore(1000);

        int k = 0;
        for (; ; ) {
            if (!semaphore.tryAcquire(1, 2, MINUTES)) {
                throw new IllegalStateException("Timeout when trying to acquire a permit!");
            }

            ICompletableFuture f = (ICompletableFuture) map.putAsync(k % 1000, k);
            f.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    semaphore.release();
                }

                @Override
                public void onFailure(Throwable t) {
                    t.printStackTrace();
                }
            });

            k++;

            if (k % 10000 == 0) {
                System.out.println("at: " + k);

            }
        }
    }
}
