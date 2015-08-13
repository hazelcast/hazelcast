package com.hazelcast.hibernate.instance;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigLoader;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.CacheEnvironment;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.hibernate.cache.CacheException;

import java.io.IOException;
import java.util.Properties;


public class HazelcastMockInstanceLoader implements IHazelcastInstanceLoader{

    private static final ILogger LOGGER = Logger.getLogger(HazelcastMockInstanceLoader.class);

    private final Properties props = new Properties();
    private String instanceName;
    private HazelcastInstance instance;
    private Config config;
    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);

    public void configure(Properties props) {
        this.props.putAll(props);
    }

    public HazelcastInstance loadInstance() throws CacheException {
        if (instance != null && instance.getLifecycleService().isRunning()) {
            LOGGER.warning("Current HazelcastInstance is already loaded and running! "
                    + "Returning current instance...");
            return instance;
        }
        String configResourcePath = null;
        instanceName = CacheEnvironment.getInstanceName(props);
        configResourcePath = CacheEnvironment.getConfigFilePath(props);
        if (!isEmpty(configResourcePath)) {
            try {
                config = ConfigLoader.load(configResourcePath);
            } catch (IOException e) {
                LOGGER.warning("IOException: " + e.getMessage());
            }
            if (config == null) {
                throw new CacheException("Could not find configuration file: "
                        + configResourcePath);
            }
        }
        if (instanceName != null) {
            instance = getHazelcastInstanceByName(instanceName);
            if (instance == null) {
                try {
                    createOrGetInstance();
                } catch (DuplicateInstanceNameException ignored) {
                    instance = getHazelcastInstanceByName(instanceName);
                }
            }
        } else {
            createOrGetInstance();
        }
        return instance;
    }

    private void createOrGetInstance() throws DuplicateInstanceNameException {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        config.setInstanceName(instanceName);
        instance = factory.newHazelcastInstance(config);
    }

    public void unloadInstance() throws CacheException {
        if (instance == null) {
            return;
        }
        final boolean shutDown = CacheEnvironment.shutdownOnStop(props, (instanceName == null));
        if (!shutDown) {
            LOGGER.warning(CacheEnvironment.SHUTDOWN_ON_STOP + " property is set to 'false'. "
                    + "Leaving current HazelcastInstance active! (Warning: Do not disable Hazelcast "
                    + GroupProperties.PROP_SHUTDOWNHOOK_ENABLED + " property!)");
            return;
        }
        try {
            instance.getLifecycleService().shutdown();
            instance = null;
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    private static boolean isEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }

    private HazelcastInstance getHazelcastInstanceByName(String instanceName) {
        HazelcastInstance foundInstance = null;
        for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
            if(instanceName.equals(instance.getName())) {
                foundInstance = instance;
            }
        }
        return foundInstance;
    }
}
