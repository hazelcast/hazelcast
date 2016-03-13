package com.hazelcast.internal.monitors;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
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

import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_ENABLED;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_METRICS_PERIOD_SECONDS;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PerformanceLogTest extends HazelcastTestSupport {

    private PerformanceLog performanceLog;
    private MetricsRegistry metricsRegistry;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(PERFORMANCE_MONITOR_ENABLED, "true");
        config.setProperty(PERFORMANCE_MONITOR_METRICS_PERIOD_SECONDS, "1");
        config.setProperty(PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB, "0.2");
        config.setProperty(PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT, "3");

        HazelcastInstance hz = createHazelcastInstance(config);

        PerformanceMonitor performanceMonitor = getPerformanceMonitor(hz);

        performanceLog = performanceMonitor.performanceLog;
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
            final File file = performanceLog.file;
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

    private static void assertNotExist(File file) {
        assertFalse("file:" + file + " should not exist", file.exists());
    }

    private static void assertExist(File file) {
        assertTrue("file:" + file + " should exist", file.exists());
    }

    @Test
    public void test() throws InterruptedException {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String content = loadLogfile();
                assertNotNull(content);

                assertTrue(content.contains("SystemProperties["));
                assertTrue(content.contains("BuildInfo["));
                assertTrue(content.contains("ConfigProperties["));
                assertTrue(content.contains("Metrics["));
            }
        });
    }

    private String loadLogfile() {
        File file = performanceLog.file;
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
                    sb.append(LINE_SEPARATOR);
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
        Node node = getNode(hazelcastInstance);
        NodeEngineImpl nodeEngine = node.nodeEngine;

        try {
            Field field = NodeEngineImpl.class.getDeclaredField("performanceMonitor");
            field.setAccessible(true);
            return (PerformanceMonitor) field.get(nodeEngine);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
