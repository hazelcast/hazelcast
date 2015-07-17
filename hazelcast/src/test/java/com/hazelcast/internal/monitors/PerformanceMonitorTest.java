package com.hazelcast.internal.monitors;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;

import static com.hazelcast.instance.GroupProperties.PROP_PERFORMANCE_MONITOR_DELAY_SECONDS;
import static com.hazelcast.instance.GroupProperties.PROP_PERFORMANCE_MONITOR_ENABLED;
import static com.hazelcast.instance.GroupProperties.PROP_PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT;
import static com.hazelcast.instance.GroupProperties.PROP_PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB;
import static com.hazelcast.instance.GroupProperties.PROP_SLOW_OPERATION_DETECTOR_ENABLED;
import static com.hazelcast.instance.GroupProperties.PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PerformanceMonitorTest extends HazelcastTestSupport {

    private PerformanceMonitor performanceMonitor;
    private PerformanceLogFile performanceLogFile;
    private InternalOperationService operationService;
    private MetricsRegistry metricsRegistry;
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(PROP_PERFORMANCE_MONITOR_ENABLED, "true");
        config.setProperty(PROP_PERFORMANCE_MONITOR_DELAY_SECONDS, "1");
        config.setProperty(PROP_PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB, "0.2");
        config.setProperty(PROP_PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT, "3");

        config.setProperty(PROP_SLOW_OPERATION_DETECTOR_ENABLED, "true");
        config.setProperty(PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, "2000");

        hz = createHazelcastInstance(config);

        performanceMonitor = getPerformanceMonitor(hz);
        performanceLogFile = performanceMonitor.performanceLogFile;
        operationService = getOperationService(hz);
        metricsRegistry = getMetricsRegistry(hz);
    }

    @AfterClass
    public static void afterClass() {
        String userDir = System.getProperty("user.dir");

        for(File file: new File(userDir).listFiles()){
            String name = file.getName();
            System.out.println(name);
            if(name.startsWith("performance-_") && name.endsWith(".log")){
                file.delete();
            }
        }
    }

    @Test
    public void testDisabledByDefault() {
        GroupProperties groupProperties = new GroupProperties(new Config());
        assertFalse(groupProperties.PERFORMANCE_MONITOR_ENABLED.getBoolean());
    }

    @Test
    public void testHazelcastConfig() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String content = loadLogfile();
                assertNotNull(content);

                assertTrue(content.contains("Hazelcast Config"));
            }
        });
    }

    @Test
    public void testMetricsRegistry() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String content = loadLogfile();
                assertNotNull(content);

                assertTrue(content.contains("Metrics"));
                assertTrue(content.contains("operation.completed.count"));
            }
        });
    }

    @Test
    public void testSystemProperties() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String content = loadLogfile();
                assertNotNull(content);
                assertTrue(content.contains("System Properties"));
                assertTrue(content.contains("java.home"));
            }
        });
    }

    @Test
    public void testBuildInfo() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String content = loadLogfile();
                assertNotNull(content);

                assertTrue(content.contains("Build Info"));
                assertTrue(content.contains("BuildNumber"));
            }
        });
    }

//    @Test
//    public void testRollover() {
//        StringBuffer sb = new StringBuffer();
//        for (int k = 0; k < 10000; k++) {
//            sb.append('a');
//        }
//
//        for (int k = 0; k < 5; k++) {
//            metricsRegistry.register(this, sb.toString() + k, new LongGauge<PerformanceMonitorTest>() {
//                @Override
//                public long get(PerformanceMonitorTest source) throws Exception {
//                    return 0;
//                }
//            });
//        }
//
//        sleepSeconds(600);
//    }

    @Test
    public void testSlowOperationTest() throws InterruptedException {
        operationService.invokeOnPartition(null, new MySlowOperation(), 1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String content = loadLogfile();
                assertNotNull(content);

                assertTrue(content.contains("Slow Operations"));
                assertTrue(content.contains("MySlowOperation"));
            }
        });
    }

    private String loadLogfile() {
        File file = performanceLogFile.logFile;
        if (file == null || !file.exists()) {
            return null;
        }

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            try {
                StringBuilder sb = new StringBuilder();
                String line = br.readLine();

                while (line != null) {
                    sb.append(line);
                    sb.append("\n");
                    line = br.readLine();
                }
                return sb.toString();
            } finally {
                br.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public PerformanceMonitor getPerformanceMonitor(HazelcastInstance hazelcastInstance) {
        try {
            Field field = HazelcastInstanceImpl.class.getDeclaredField("performanceMonitor");
            Node node = getNode(hazelcastInstance);
            field.setAccessible(true);
            return (PerformanceMonitor) field.get(node.hazelcastInstance);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class MySlowOperation extends AbstractOperation {
        @Override
        public void run() throws Exception {
            Thread.sleep(10000);
        }
    }
}
