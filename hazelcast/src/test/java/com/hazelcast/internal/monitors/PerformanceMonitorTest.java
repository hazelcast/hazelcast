package com.hazelcast.internal.monitors;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
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
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_DELAY_SECONDS;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_ENABLED;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB;
import static com.hazelcast.instance.GroupProperty.SLOW_OPERATION_DETECTOR_ENABLED;
import static com.hazelcast.instance.GroupProperty.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS;
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
        config.setProperty(PERFORMANCE_MONITOR_ENABLED, "true");
        config.setProperty(PERFORMANCE_MONITOR_DELAY_SECONDS, "1");
        config.setProperty(PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB, "0.2");
        config.setProperty(PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT, "3");

        config.setProperty(SLOW_OPERATION_DETECTOR_ENABLED, "true");
        config.setProperty(SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, "2000");

        hz = createHazelcastInstance(config);

        performanceMonitor = getPerformanceMonitor(hz);
        performanceLogFile = performanceMonitor.performanceLogFile;
        operationService = getOperationService(hz);
        metricsRegistry = getMetricsRegistry(hz);
    }

    @AfterClass
    public static void afterClass() {
        String userDir = System.getProperty("user.dir");

        File[] files = new File(userDir).listFiles();
        if (files != null) {
            for (File file : files) {
                String name = file.getName();
                if (name.startsWith("performance-") && name.endsWith(".log")) {
                    file.delete();
                }
            }
        }
    }

    @Test
    public void testDisabledByDefault() {
        GroupProperties groupProperties = new GroupProperties(new Config());
        assertFalse(groupProperties.getBoolean(PERFORMANCE_MONITOR_ENABLED));
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

    @Test
    public void testRollover() {
        String id = generateRandomString(10000);

        final List<File> files = new LinkedList<File>();

        LongProbeFunction f = new LongProbeFunction() {
            @Override
            public long get(Object source) throws Exception {
                return 0;
            }
        };

        for (int k = 0; k < 10; k++) {
            metricsRegistry.register(this, id + k, ProbeLevel.MANDATORY, f);
        }

        // we run for some time to make sure we get enough rollovers.
        while (files.size() < 3) {
            final File file = performanceLogFile.logFile;
            if (file != null) {
                if (!files.contains(file)) {
                    files.add(file);
                }

                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertExist(file);
                    }
                });
            }

            sleepMillis(100);
        }

        // eventually all these files should be gone.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (File file : files) {
                    assertNotExist(file);
                }
            }
        });
    }

    private void assertNotExist(File file) {
        assertFalse("file:" + file + " should not exist", file.exists());
    }

    private void assertExist(File file) {
        assertTrue("file:" + file + " should exist", file.exists());
    }

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
