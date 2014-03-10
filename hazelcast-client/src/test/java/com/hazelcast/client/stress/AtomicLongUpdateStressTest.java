package com.hazelcast.client.stress;

         
         import com.hazelcast.client.HazelcastClient;
         import com.hazelcast.client.config.ClientConfig;
         import com.hazelcast.client.stress.support.StressTestSupport;
         import com.hazelcast.client.stress.support.TestThread;
         import com.hazelcast.core.HazelcastInstance;
         import com.hazelcast.core.IAtomicLong;
         import com.hazelcast.test.HazelcastSerialClassRunner;
         import com.hazelcast.test.annotation.SlowTest;
         import org.junit.After;
         import org.junit.Before;
         import org.junit.Test;
         import org.junit.experimental.categories.Category;
         import org.junit.runner.RunWith;
         
         import java.util.HashSet;
         import java.util.Set;
         
         import static org.junit.Assert.fail;
         
         /**
   * This test fails sporadically. It seems to indicate a problem within the core because there is not much logic
   * in the atomicwrapper that can fail (just increment).
   */
         @RunWith(HazelcastSerialClassRunner.class)
 @Category(SlowTest.class)
 public class AtomicLongUpdateStressTest extends StressTestSupport<AtomicLongUpdateStressTest.StressThread> {
     
     public static final int CLIENT_THREAD_COUNT = 25;
     public static final int REFERENCE_COUNT = 1;//10 * 1000;
     private IAtomicLong[] references  = new IAtomicLong[REFERENCE_COUNT];


     @Before
     public void setUp() {
         super.TOTAL_HZ_INSTANCES = 1;
         super.THREADS_PER_INSTANCE = CLIENT_THREAD_COUNT;

         cluster.initCluster();

         ClientConfig clientConfig = new ClientConfig();
         clientConfig.getNetworkConfig().setRedoOperation(true);

         setClientConfig(clientConfig);
         initStressThreadsWithClient(this);

         for (int k = 0; k < references.length; k++) {
            references[k] = stressThreads.get(0).instance.getAtomicLong("atomicreference:" + k);
         }
     }
     

     @Test
     public void testChangingCluster() {
         setKillThread(new KillMemberOwningKeyThread("atomicreference:" + 0));
         runTest(true);
     }

     @Test
     public void testFixedCluster() {
         runTest(false);
     }

     public void assertResult() {

         int[] increments = new int[REFERENCE_COUNT];
         for (StressThread t : stressThreads) {
             t.addIncrements(increments);
         }

         Set<Integer> failedKeys = new HashSet<Integer>();
         for (int k = 0; k < REFERENCE_COUNT; k++) {
             long expectedValue = increments[k];
             long foundValue = references[k].get();
             if (expectedValue != foundValue) {
                 failedKeys.add(k);
             }
         }

         if (failedKeys.isEmpty()) {
             return;
         }

         int index = 1;
         for (Integer key : failedKeys) {
             System.err.println("Failed write: " + index + " found:" + references[key].get() + " expected:" + increments[key]);
             index++;
         }

         fail("There are failed writes, number of failures:" + failedKeys.size());
     }
     
     public class StressThread extends TestThread {
         private final int[] increments = new int[REFERENCE_COUNT];

         public StressThread(HazelcastInstance node){
             super(node);
         }

         public void testLoop() throws Exception {

            int index = random.nextInt(REFERENCE_COUNT);
            int increment = random.nextInt(100);
            increments[index] += increment;
            IAtomicLong reference = references[index];
            reference.addAndGet(increment);

         }

         public void addIncrements(int[] increments) {
             for (int k = 0; k < increments.length; k++) {
                 increments[k] += this.increments[k];
             }
         }
     }
 }