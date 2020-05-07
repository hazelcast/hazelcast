package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.System.getenv;

public class PhoneHome {

    private static final int TIMEOUT = 1000;

    private static final int A_INTERVAL = 5;
    private static final int B_INTERVAL = 10;
    private static final int C_INTERVAL = 20;
    private static final int D_INTERVAL = 40;
    private static final int E_INTERVAL = 60;
    private static final int F_INTERVAL = 100;
    private static final int G_INTERVAL = 150;
    private static final int H_INTERVAL = 300;
    private static final int J_INTERVAL = 600;

    private static final String BASE_PHONE_HOME_URL = "http://phonehome.hazelcast.com/ping";
    private static final int CONNECTION_TIMEOUT_MILLIS = 3000;
    private static final String FALSE = "false";

    volatile ScheduledFuture<?> phoneHomeFuture;
    private final ILogger logger;
    private final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
    private final Node hazelcastNode;

    public PhoneHome(Node node) {
        hazelcastNode = node;
        logger = hazelcastNode.getLogger(com.hazelcast.internal.util.phonehome.PhoneHome.class);
    }

    public void check() {
        if (!hazelcastNode.getProperties().getBoolean(ClusterProperty.PHONE_HOME_ENABLED)) {
            return;
        }
        if (FALSE.equals(getenv("HZ_PHONE_HOME_ENABLED"))) {
            return;
        }
        try {
            phoneHomeFuture = hazelcastNode.nodeEngine.getExecutionService()
                    .scheduleWithRepetition("PhoneHome",
                            () -> phoneHome(false), 0, 1, TimeUnit.DAYS);
        } catch (RejectedExecutionException e) {
            logger.warning("Could not schedule phone home task! Most probably Hazelcast failed to start.");
        }
    }

    public void shutdown() {
        if (phoneHomeFuture != null) {
            phoneHomeFuture.cancel(true);
        }
    }

    public String convertToLetter(int size) {
        String letter;
        if (size < A_INTERVAL) {
            letter = "A";
        } else if (size < B_INTERVAL) {
            letter = "B";
        } else if (size < C_INTERVAL) {
            letter = "C";
        } else if (size < D_INTERVAL) {
            letter = "D";
        } else if (size < E_INTERVAL) {
            letter = "E";
        } else if (size < F_INTERVAL) {
            letter = "F";
        } else if (size < G_INTERVAL) {
            letter = "G";
        } else if (size < H_INTERVAL) {
            letter = "H";
        } else if (size < J_INTERVAL) {
            letter = "J";
        } else {
            letter = "I";
        }
        return letter;
    }

    /**
     * Performs a phone request for {@code node} and returns the generated request
     * parameters. If {@code pretend} is {@code true}, only returns the parameters
     * without actually performing the request.
     *
     * @param pretend if {@code true}, do not perform the request
     * @return the generated request parameters
     */
    public Map<String, String> phoneHome(boolean pretend) {
        PhoneHomeParameterCreator parameterCreator = createParameters();

        if (!pretend) {
            String urlStr = BASE_PHONE_HOME_URL + parameterCreator.build();
            fetchWebService(urlStr);
        }

        return parameterCreator.getParameters();
    }

    public PhoneHomeParameterCreator createParameters() {
        ClusterServiceImpl clusterService = hazelcastNode.getClusterService();
        int clusterSize = clusterService.getMembers().size();
        Long clusterUpTime = clusterService.getClusterClock().getClusterUpTime();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        JetBuildInfo jetBuildInfo = hazelcastNode.getBuildInfo().getJetBuildInfo();

        PhoneHomeParameterCreator parameterCreator = new PhoneHomeParameterCreator()
                .addParam("version", buildInfo.getVersion())
                .addParam("m", hazelcastNode.getThisUuid().toString())
                .addParam("p", getDownloadId())
                .addParam("c", clusterService.getClusterId().toString())
                .addParam("crsz", convertToLetter(clusterSize))
                .addParam("cssz", convertToLetter(hazelcastNode.clientEngine.getClientEndpointCount()))
                .addParam("cuptm", Long.toString(clusterUpTime))
                .addParam("nuptm", Long.toString(runtimeMxBean.getUptime()))
                .addParam("jvmn", runtimeMxBean.getVmName())
                .addParam("jvmv", System.getProperty("java.version"))
                .addParam("jetv", jetBuildInfo == null ? "" : jetBuildInfo.getVersion());
        addClientInfo(parameterCreator);
        addOSInfo(parameterCreator);

        return parameterCreator;
    }

    private String getDownloadId() {
        String downloadId = "source";
        InputStream is = null;
        try {
            is = getClass().getClassLoader().getResourceAsStream("hazelcast-download.properties");
            if (is != null) {
                final Properties properties = new Properties();
                properties.load(is);
                downloadId = properties.getProperty("hazelcastDownloadId");
            }
        } catch (IOException ignored) {
            ignore(ignored);
        } finally {
            closeResource(is);
        }
        return downloadId;
    }

    private void fetchWebService(String urlStr) {
        InputStream in = null;
        try {
            URL url = new URL(urlStr);
            URLConnection conn = url.openConnection();
            conn.setRequestProperty("User-Agent", "Mozilla/5.0");
            conn.setConnectTimeout(TIMEOUT * 2);
            conn.setReadTimeout(TIMEOUT * 2);
            in = new BufferedInputStream(conn.getInputStream());
        } catch (Exception ignored) {
            ignore(ignored);
        } finally {
            closeResource(in);
        }
    }

    private void addOSInfo(PhoneHomeParameterCreator parameterCreator) {
        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        try {
            parameterCreator
                    .addParam("osn", osMxBean.getName())
                    .addParam("osa", osMxBean.getArch())
                    .addParam("osv", osMxBean.getVersion());
        } catch (SecurityException e) {
            parameterCreator
                    .addParam("osn", "N/A")
                    .addParam("osa", "N/A")
                    .addParam("osv", "N/A");
        }
    }

    private void addClientInfo(PhoneHomeParameterCreator parameterCreator) {
        Map<String, Integer> clusterClientStats = hazelcastNode.clientEngine.getConnectedClientStats();
        parameterCreator
                .addParam("ccpp", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.CPP_CLIENT, 0)))
                .addParam("cdn", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.CSHARP_CLIENT, 0)))
                .addParam("cjv", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.JAVA_CLIENT, 0)))
                .addParam("cnjs", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.NODEJS_CLIENT, 0)))
                .addParam("cpy", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.PYTHON_CLIENT, 0)))
                .addParam("cgo", Integer.toString(clusterClientStats.getOrDefault(ConnectionType.GO_CLIENT, 0)));
    }

    private void checkClusterSizeAndSetLicense(int clusterSize, PhoneHomeParameterCreator parameterCreator) {
        if (clusterSize <= 2) {
            parameterCreator.addParam("mclicense", "MC_LICENSE_NOT_REQUIRED");
        } else {
            parameterCreator.addParam("mclicense", "MC_LICENSE_REQUIRED_BUT_NOT_SET");
        }
    }
}
