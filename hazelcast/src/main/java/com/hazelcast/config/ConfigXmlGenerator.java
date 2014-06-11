/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * The ConfigXmlGenerator is responsible for transforming a {@link Config} to a Hazelcast XML string.
 */
public class ConfigXmlGenerator {

    private static final ILogger LOGGER = Logger.getLogger(ConfigXmlGenerator.class);

    private static final int INDENT = 5;

    private final boolean formatted;

    /**
     * Creates a ConfigXmlGenerator that will format the code.
     */
    public ConfigXmlGenerator() {
        this(true);
    }

    /**
     * Creates a ConfigXmlGenerator.
     *
     * @param formatted true if the XML should be formatted, false otherwise.
     */
    public ConfigXmlGenerator(boolean formatted) {
        this.formatted = formatted;
    }

    /**
     * Generates the XML string based on some Config.
     *
     * @param config the configuration.
     * @return the XML string.
     */
    public String generate(Config config) {
        isNotNull(config, "Config");

        final StringBuilder xml = new StringBuilder();
        xml.append("<hazelcast ")
                .append("xmlns=\"http://www.hazelcast.com/schema/config\"\n")
                .append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n")
                .append("xsi:schemaLocation=\"http://www.hazelcast.com/schema/config ")
                .append("http://www.hazelcast.com/schema/config/hazelcast-config-3.3.xsd\">");
        xml.append("<group>");
        xml.append("<name>").append(config.getGroupConfig().getName()).append("</name>");
        xml.append("<password>").append(config.getGroupConfig().getPassword()).append("</password>");
        xml.append("</group>");
        if (config.getLicenseKey() != null) {
            xml.append("<license-key>").append(config.getLicenseKey()).append("</license-key>");
        }
        if (config.getManagementCenterConfig() != null) {
            ManagementCenterConfig mcConfig = config.getManagementCenterConfig();
            xml.append("<management-center enabled=\"").append(mcConfig.isEnabled())
                    .append("\" update-interval=\"").append(mcConfig.getUpdateInterval()).append("\">")
                    .append(mcConfig.getUrl()).append("</management-center>");
        }
        appendProperties(xml, config.getProperties());
        final Collection<WanReplicationConfig> wanRepConfigs = config.getWanReplicationConfigs().values();
        for (WanReplicationConfig wan : wanRepConfigs) {
            xml.append("<wan-replication name=\"").append(wan.getName()).append("\">");
            final List<WanTargetClusterConfig> targets = wan.getTargetClusterConfigs();
            for (WanTargetClusterConfig t : targets) {
                xml.append("<target-cluster group-name=\"").append(t.getGroupName())
                        .append("\" group-password=\"").append(t.getGroupPassword()).append("\">");
                xml.append("<replication-impl>").append(t.getReplicationImpl()).append("</replication-impl>");
                xml.append("<end-points>");
                final List<String> eps = t.getEndpoints();
                for (String ep : eps) {
                    xml.append("<address>").append(ep).append("</address>");
                }
                xml.append("</end-points>").append("</target-cluster>");
            }
            xml.append("</wan-replication>");
        }
        final NetworkConfig netCfg = config.getNetworkConfig();
        xml.append("<network>");
        if (netCfg.getPublicAddress() != null) {
            xml.append("<public-address>").append(netCfg.getPublicAddress()).append("</public-address>");
        }
        xml.append("<port port-count=\"").append(netCfg.getPortCount()).append("\" ")
                .append("auto-increment=\"").append(netCfg.isPortAutoIncrement()).append("\">")
                .append(netCfg.getPort()).append("</port>");
        final JoinConfig join = netCfg.getJoin();
        xml.append("<join>");
        final MulticastConfig mcast = join.getMulticastConfig();
        xml.append("<multicast enabled=\"").append(mcast.isEnabled()).append("\">");
        xml.append("<multicast-group>").append(mcast.getMulticastGroup()).append("</multicast-group>");
        xml.append("<multicast-port>").append(mcast.getMulticastPort()).append("</multicast-port>");
        xml.append("<multicast-timeout-seconds>").append(mcast.getMulticastTimeoutSeconds())
                .append("</multicast-timeout-seconds>");
        xml.append("<multicast-time-to-live>").append(mcast.getMulticastTimeToLive())
                .append("</multicast-time-to-live>");
        if (!mcast.getTrustedInterfaces().isEmpty()) {
            xml.append("<trusted-interfaces>");
            for (String trustedInterface : mcast.getTrustedInterfaces()) {
                xml.append("<interface>").append(trustedInterface).append("</interface>");
            }
            xml.append("</trusted-interfaces>");
        }
        xml.append("</multicast>");
        final TcpIpConfig tcpCfg = join.getTcpIpConfig();
        xml.append("<tcp-ip enabled=\"").append(tcpCfg.isEnabled()).append("\">");
        final List<String> members = tcpCfg.getMembers();
        for (String m : members) {
            xml.append("<member>").append(m).append("</member>");
        }
        if (tcpCfg.getRequiredMember() != null) {
            xml.append("<required-member>").append(tcpCfg.getRequiredMember()).append("</required-member>");
        }
        xml.append("</tcp-ip>");
        final AwsConfig awsConfig = join.getAwsConfig();
        xml.append("<aws enabled=\"").append(awsConfig.isEnabled()).append("\">");
        xml.append("<access-key>").append(awsConfig.getAccessKey()).append("</access-key>");
        xml.append("<secret-key>").append(awsConfig.getSecretKey()).append("</secret-key>");
        xml.append("<region>").append(awsConfig.getRegion()).append("</region>");
        xml.append("<security-group-name>").append(awsConfig.getSecurityGroupName()).append("</security-group-name>");
        xml.append("<tag-key>").append(awsConfig.getTagKey()).append("</tag-key>");
        xml.append("<tag-value>").append(awsConfig.getTagValue()).append("</tag-value>");
        xml.append("</aws>");
        xml.append("</join>");
        final InterfacesConfig interfaces = netCfg.getInterfaces();
        xml.append("<interfaces enabled=\"").append(interfaces.isEnabled()).append("\">");
        final Collection<String> interfaceList = interfaces.getInterfaces();
        for (String i : interfaceList) {
            xml.append("<interface>").append(i).append("</interface>");
        }
        xml.append("</interfaces>");
        final SSLConfig ssl = netCfg.getSSLConfig();
        xml.append("<ssl enabled=\"").append(ssl != null && ssl.isEnabled()).append("\">");
        if (ssl != null) {
            String className = ssl.getFactoryImplementation() != null
                    ? ssl.getFactoryImplementation().getClass().getName()
                    : ssl.getFactoryClassName();
            xml.append("<factory-class-name>").append(className).append("</factory-class-name>");
            appendProperties(xml, ssl.getProperties());
        }
        xml.append("</ssl>");
        final SocketInterceptorConfig socket = netCfg.getSocketInterceptorConfig();
        xml.append("<socket-interceptor enabled=\"").append(socket != null && socket.isEnabled()).append("\">");
        if (socket != null) {
            String className = socket.getImplementation() != null
                    ? socket.getImplementation().getClass().getName() : socket.getClassName();
            xml.append("<class-name>").append(className).append("</class-name>");
            appendProperties(xml, socket.getProperties());
        }
        xml.append("</socket-interceptor>");
        final SymmetricEncryptionConfig sec = netCfg.getSymmetricEncryptionConfig();
        xml.append("<symmetric-encryption enabled=\"").append(sec != null && sec.isEnabled()).append("\">");
        if (sec != null) {
            xml.append("<algorithm>").append(sec.getAlgorithm()).append("</algorithm>");
            xml.append("<salt>").append(sec.getSalt()).append("</salt>");
            xml.append("<password>").append(sec.getPassword()).append("</password>");
            xml.append("<iteration-count>").append(sec.getIterationCount()).append("</iteration-count>");
        }
        xml.append("</symmetric-encryption>");
        xml.append("</network>");

        final PartitionGroupConfig pg = config.getPartitionGroupConfig();
        if (pg != null) {
            xml.append("<partition-group enabled=\"").append(pg.isEnabled())
                    .append("\" group-type=\"").append(pg.getGroupType()).append("\" />");
        }

        final Collection<ExecutorConfig> exCfgs = config.getExecutorConfigs().values();
        for (ExecutorConfig ex : exCfgs) {
            xml.append("<executor-service name=\"").append(ex.getName()).append("\">");
            xml.append("<pool-size>").append(ex.getPoolSize()).append("</pool-size>");
            xml.append("<queue-capacity>").append(ex.getQueueCapacity()).append("</queue-capacity>");
            xml.append("</executor-service>");
        }
        final Collection<QueueConfig> qCfgs = config.getQueueConfigs().values();
        for (QueueConfig q : qCfgs) {
            xml.append("<queue name=\"").append(q.getName()).append("\">");
            xml.append("<queue-max-size>").append(q.getMaxSize()).append("</queue-max-size>");
            xml.append("<queue-sync-backup-count>").append(q.getBackupCount()).append("</queue-sync-backup-count>");
            xml.append("<queue-async-backup-count>").append(q.getAsyncBackupCount()).append("</queue-async-backup-count>");
            if (!q.getItemListenerConfigs().isEmpty()) {
                xml.append("<item-listeners>");
                for (ItemListenerConfig lc : q.getItemListenerConfigs()) {
                    xml.append("<item-listener include-value=\"").append(lc.isIncludeValue()).append("\">");
                    xml.append(lc.getClassName());
                    xml.append("</item-listener>");
                }
                xml.append("</item-listeners>");
            }
            xml.append("</queue>");
        }
        final Collection<MapConfig> mCfgs = config.getMapConfigs().values();
        for (MapConfig m : mCfgs) {
            xml.append("<map name=\"").append(m.getName()).append("\">");
            xml.append("<in-memory-format>").append(m.getInMemoryFormat())
                    .append("</in-memory-format>");
            xml.append("<backup-count>").append(m.getBackupCount())
                    .append("</backup-count>");
            xml.append("<async-backup-count>").append(m.getAsyncBackupCount())
                    .append("</async-backup-count>");
            xml.append("<time-to-live-seconds>").append(m.getTimeToLiveSeconds())
                    .append("</time-to-live-seconds>");
            xml.append("<max-idle-seconds>").append(m.getMaxIdleSeconds())
                    .append("</max-idle-seconds>");
            xml.append("<eviction-policy>").append(m.getEvictionPolicy())
                    .append("</eviction-policy>");
            xml.append("<max-size policy=\"").append(m.getMaxSizeConfig().getMaxSizePolicy())
                    .append("\">").append(m.getMaxSizeConfig().getSize())
                    .append("</max-size>");
            xml.append("<eviction-percentage>").append(m.getEvictionPercentage()).append("</eviction-percentage>");
            xml.append("<merge-policy>").append(m.getMergePolicy())
                    .append("</merge-policy>");
            xml.append("<read-backup-data>").append(m.isReadBackupData())
                    .append("</read-backup-data>");
            xml.append("<statistics-enabled>").append(m.isStatisticsEnabled())
                    .append("</statistics-enabled>");
            if (m.getMapStoreConfig() != null) {
                final MapStoreConfig s = m.getMapStoreConfig();
                xml.append("<map-store enabled=\"").append(s.isEnabled())
                        .append("\">");
                final String clazz = s.getImplementation()
                        != null ? s.getImplementation().getClass().getName() : s.getClassName();
                xml.append("<class-name>").append(clazz).append("</class-name>");
                final String factoryClass = s.getFactoryImplementation() != null
                        ? s.getFactoryImplementation().getClass().getName()
                        : s.getFactoryClassName();
                if (factoryClass != null) {
                    xml.append("<factory-class-name>").append(factoryClass).append("</factory-class-name>");
                }
                xml.append("<write-delay-seconds>").append(s.getWriteDelaySeconds()).append("</write-delay-seconds>");
                appendProperties(xml, s.getProperties());
                xml.append("</map-store>");
            }
            if (m.getNearCacheConfig() != null) {
                final NearCacheConfig n = m.getNearCacheConfig();
                xml.append("<near-cache>");
                xml.append("<max-size>").append(n.getMaxSize()).append("</max-size>");
                xml.append("<time-to-live-seconds>").append(n.getTimeToLiveSeconds()).append("</time-to-live-seconds>");
                xml.append("<max-idle-seconds>").append(n.getMaxIdleSeconds()).append("</max-idle-seconds>");
                xml.append("<eviction-policy>").append(n.getEvictionPolicy()).append("</eviction-policy>");
                xml.append("<invalidate-on-change>").append(n.isInvalidateOnChange()).append("</invalidate-on-change>");
                xml.append("<in-memory-format>").append(n.getInMemoryFormat()).append("</in-memory-format>");
                xml.append("</near-cache>");
            }
            if (m.getWanReplicationRef() != null) {
                final WanReplicationRef wan = m.getWanReplicationRef();
                xml.append("<wan-replication-ref name=\"").append(wan.getName()).append("\">");
                xml.append("<merge-policy>").append(wan.getMergePolicy()).append("</merge-policy>");
                xml.append("</wan-replication-ref>");
            }
            if (!m.getMapIndexConfigs().isEmpty()) {
                xml.append("<indexes>");
                for (MapIndexConfig indexCfg : m.getMapIndexConfigs()) {
                    xml.append("<index ordered=\"").append(indexCfg.isOrdered()).append("\">");
                    xml.append(indexCfg.getAttribute());
                    xml.append("</index>");
                }
                xml.append("</indexes>");
            }
            if (!m.getEntryListenerConfigs().isEmpty()) {
                xml.append("<entry-listeners>");
                for (EntryListenerConfig lc : m.getEntryListenerConfigs()) {
                    xml.append("<entry-listener include-value=\"").append(lc.isIncludeValue())
                            .append("\" local=\"").append(lc.isLocal()).append("\">");
                    final String clazz = lc.getImplementation()
                            != null ? lc.getImplementation().getClass().getName() : lc.getClassName();
                    xml.append(clazz);
                    xml.append("</entry-listener>");
                }
                xml.append("</entry-listeners>");
            }
            if (m.getPartitioningStrategyConfig() != null) {
                xml.append("<partition-strategy>");
                PartitioningStrategyConfig psc = m.getPartitioningStrategyConfig();
                if (psc.getPartitioningStrategy() != null) {
                    xml.append(psc.getPartitioningStrategy().getClass().getName());
                } else {
                    xml.append(psc.getPartitioningStrategyClass());
                }
                xml.append("</partition-strategy>");
            }
            xml.append("</map>");
        }
        final Collection<MultiMapConfig> mmCfgs = config.getMultiMapConfigs().values();
        for (MultiMapConfig mm : mmCfgs) {
            xml.append("<multimap name=\"").append(mm.getName()).append("\">");
            xml.append("<value-collection-type>").append(mm.getValueCollectionType()).append("</value-collection-type>");
            if (!mm.getEntryListenerConfigs().isEmpty()) {
                xml.append("<entry-listeners>");
                for (EntryListenerConfig lc : mm.getEntryListenerConfigs()) {
                    xml.append("<entry-listener include-value=\"").append(lc.isIncludeValue())
                            .append("\" local=\"").append(lc.isLocal()).append("\">");
                    final String clazz = lc.getImplementation()
                            != null ? lc.getImplementation().getClass().getName() : lc.getClassName();
                    xml.append(clazz);
                    xml.append("</entry-listener>");
                }
                xml.append("</entry-listeners>");
            }
//            if (mm.getPartitioningStrategyConfig() != null) {
//                xml.append("<partition-strategy>");
//                PartitioningStrategyConfig psc = mm.getPartitioningStrategyConfig();
//                if (psc.getPartitioningStrategy() != null) {
//                    xml.append(psc.getPartitioningStrategy().getClass().getName());
//                } else {
//                    xml.append(psc.getPartitioningStrategyClass());
//                }
//                xml.append("</partition-strategy>");
//            }
            xml.append("</multimap>");
        }
        final Collection<TopicConfig> tCfgs = config.getTopicConfigs().values();
        for (TopicConfig t : tCfgs) {
            xml.append("<topic name=\"").append(t.getName()).append("\">");
            xml.append("<global-ordering-enabled>").append(t.isGlobalOrderingEnabled())
                    .append("</global-ordering-enabled>");
            if (!t.getMessageListenerConfigs().isEmpty()) {
                xml.append("<message-listeners>");
                for (ListenerConfig lc : t.getMessageListenerConfigs()) {
                    xml.append("<message-listener>");
                    final String clazz = lc.getImplementation()
                            != null ? lc.getImplementation().getClass().getName() : lc.getClassName();
                    xml.append(clazz);
                    xml.append("</message-listener>");
                }
                xml.append("</message-listeners>");
            }
            xml.append("</topic>");
        }
        final Collection<SemaphoreConfig> semaphoreCfgs = config.getSemaphoreConfigs();
        for (SemaphoreConfig sc : semaphoreCfgs) {
            xml.append("<semaphore name=\"").append(sc.getName()).append("\">");
            xml.append("<initial-permits>").append(sc.getInitialPermits()).append("</initial-permits>");
            xml.append("<backup-count>").append(sc.getBackupCount()).append("</backup-count>");
            xml.append("<async-backup-count>").append(sc.getAsyncBackupCount()).append("</async-backup-count>");
            xml.append("</semaphore>");
        }
        if (!config.getListenerConfigs().isEmpty()) {
            xml.append("<listeners>");
            for (ListenerConfig lc : config.getListenerConfigs()) {
                xml.append("<listener>");
                final String clazz = lc.getImplementation()
                        != null ? lc.getImplementation().getClass().getName() : lc.getClassName();
                xml.append(clazz);
                xml.append("</listener>");
            }
            xml.append("</listeners>");
        }
        xml.append("</hazelcast>");
        return format(xml.toString(), INDENT);
    }

    private String format(final String input, int indent) {
        if (!formatted) {
            return input;
        }
        try {
            final Source xmlInput = new StreamSource(new StringReader(input));
            final StreamResult xmlOutput = new StreamResult(new StringWriter());
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            /* Older versions of Xalan still use this method of setting indent values.
            * Attempt to make this work but don't completely fail if it's a problem.
            */
            try {
                transformerFactory.setAttribute("indent-number", indent);
            } catch (IllegalArgumentException e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Failed to set indent-number attribute; cause: " + e.getMessage());
                }
            }
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            /* Newer versions of Xalan will look for a fully-qualified output property in order to specify amount of
            * indentation to use.  Attempt to make this work as well but again don't completely fail if it's a problem.
            */
            try {
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", Integer.toString(indent));
            } catch (IllegalArgumentException e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Failed to set indent-amount property; cause: " + e.getMessage());
                }
            }
            transformer.transform(xmlInput, xmlOutput);
            return xmlOutput.getWriter().toString();
        } catch (Exception e) {
            LOGGER.warning(e);
            return input;
        }
    }

    private void appendProperties(StringBuilder xml, Properties props) {
        if (!props.isEmpty()) {
            xml.append("<properties>");
            Set keys = props.keySet();
            for (Object key : keys) {
                xml.append("<property name=\"").append(key).append("\">")
                        .append(props.getProperty(key.toString()))
                        .append("</property>");
            }
            xml.append("</properties>");
        }
    }
}
