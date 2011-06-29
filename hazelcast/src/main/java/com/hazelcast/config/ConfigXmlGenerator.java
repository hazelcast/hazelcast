package com.hazelcast.config;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import com.hazelcast.nio.Address;

public class ConfigXmlGenerator {
	
	private final boolean formatted;
	
	public ConfigXmlGenerator() {
		this(true);
	}
	
	public ConfigXmlGenerator(boolean formatted) {
		this.formatted = formatted;
	}
	
	public String generate(Config config) {
		final StringBuilder xml = new StringBuilder();
		xml.append("<hazelcast>");
		
		xml.append("<group>");
		xml.append("<name>").append(config.getGroupConfig().getName()).append("</name>");
		xml.append("<password>").append(config.getGroupConfig().getPassword()).append("</password>");
		xml.append("</group>");

		appendProperties(xml, config.getProperties());
		
		final NetworkConfig netCfg = config.getNetworkConfig();
		xml.append("<network>");
		xml.append("<port auto-increment=\"").append(config.isPortAutoIncrement()).append("\">")
			.append(config.getPort()).append("</port>");
		
		final Join join = netCfg.getJoin();
		xml.append("<join>");
		final MulticastConfig mcast = join.getMulticastConfig();
		xml.append("<multicast enabled=\"").append(mcast.isEnabled()).append("\">") ;
		xml.append("<multicast-group>").append(mcast.getMulticastGroup()).append("</multicast-group>");
		xml.append("<multicast-port>").append(mcast.getMulticastPort()).append("</multicast-port>");
		xml.append("<multicast-timeout-seconds>").append(mcast.getMulticastTimeoutSeconds()).append("</multicast-timeout-seconds>");
		xml.append("</multicast>");
			
		final TcpIpConfig tcpCfg = join.getTcpIpConfig();
		xml.append("<tcp-ip enabled=\"").append(tcpCfg.isEnabled()).append("\">") ;
		final List<String> members = tcpCfg.getMembers();
		for (String m : members) {
			xml.append("<interface>").append(m).append("</interface>");
		}
		final List<Address> addresses = tcpCfg.getAddresses();
		for (Address a : addresses) {
			xml.append("<address>").append(a.getHost()).append(":").append(a.getPort()).append("</address>");
		}
		if(tcpCfg.getRequiredMember() != null) {
			xml.append("<required-member>").append(tcpCfg.getRequiredMember()).append("</required-member>");
		}
		xml.append("</tcp-ip>");
		xml.append("</join>");
		
		final Interfaces interfaces = netCfg.getInterfaces();
		xml.append("<interfaces enabled=\"").append(interfaces.isEnabled()).append("\">") ;
		final Collection<String> interfaceList = interfaces.getInterfaces();
		for (String i : interfaceList) {
			xml.append("<interface>").append(i).append("</interface>");
		}
		xml.append("</interfaces>");
		
		final SymmetricEncryptionConfig sec = netCfg.getSymmetricEncryptionConfig();
		xml.append("<symmetric-encryption enabled=\"").append(sec != null && sec.isEnabled()).append("\">");
		if(sec != null) {
			xml.append("<algorithm>").append(sec.getAlgorithm()).append("</algorithm>");
			xml.append("<salt>").append(sec.getSalt()).append("</salt>");
			xml.append("<password>").append(sec.getPassword()).append("</password>");
			xml.append("<iteration-count>").append(sec.getIterationCount()).append("</iteration-count>");
		}
		xml.append("</symmetric-encryption>");
		
		final AsymmetricEncryptionConfig asec = netCfg.getAsymmetricEncryptionConfig();
		xml.append("<asymmetric-encryption enabled=\"").append(asec != null && asec.isEnabled()).append("\">");
		if(asec != null) {
			xml.append("<algorithm>").append(asec.getAlgorithm()).append("</algorithm>");
			xml.append("<keyPassword>").append(asec.getKeyPassword()).append("</keyPassword>");
			xml.append("<keyAlias>").append(asec.getKeyAlias()).append("</keyAlias>");
			xml.append("<storeType>").append(asec.getStoreType()).append("</storeType>");
			xml.append("<storePassword>").append(asec.getStorePassword()).append("</storePassword>");
			xml.append("<storePath>").append(asec.getStorePath()).append("</storePath>");
		}
		xml.append("</asymmetric-encryption>");
		xml.append("</network>");

		final Collection<ExecutorConfig> exCfgs = config.getExecutorConfigs();
		for (ExecutorConfig ex : exCfgs) {
			xml.append("<executor-service name=\"").append(ex.getName()).append("\">");
			xml.append("<core-pool-size>").append(ex.getCorePoolSize()).append("</core-pool-size>");
			xml.append("<max-pool-size>").append(ex.getMaxPoolSize()).append("</max-pool-size>");
			xml.append("<keep-alive-seconds>").append(ex.getKeepAliveSeconds()).append("</keep-alive-seconds>");
			xml.append("</executor-service>");
		}
		
		final Collection<QueueConfig> qCfgs = config.getQConfigs().values();
		for (QueueConfig q : qCfgs) {
			xml.append("<queue name=\"").append(q.getName()).append("\">");
			xml.append("<max-size-per-jvm>").append(q.getMaxSizePerJVM()).append("</max-size-per-jvm>");
			xml.append("<backing-map-ref>").append(q.getBackingMapRef()).append("</backing-map-ref>");
			xml.append("</queue>");
		}
		
		final Collection<TopicConfig> tCfgs = config.getTopicConfigs().values();
		for (TopicConfig t : tCfgs) {
			xml.append("<topic name=\"").append(t.getName()).append("\">");
			xml.append("<global-ordering-enabled>").append(t.isGlobalOrderingEnabled()).append("</global-ordering-enabled>");
			xml.append("</TopicConfig>");
		}
		
		final Collection<MapConfig> mCfgs = config.getMapConfigs().values();
		for (MapConfig m : mCfgs) {
			xml.append("<map name=\"").append(m.getName()).append("\">");
			xml.append("<backup-count>").append(m.getBackupCount()).append("</backup-count>");
			xml.append("<eviction-policy>").append(m.getEvictionPolicy()).append("</eviction-policy>");
			xml.append("<eviction-percentage>").append(m.getEvictionPercentage()).append("</eviction-percentage>");
			xml.append("<eviction-delay-seconds>").append(m.getEvictionDelaySeconds()).append("</eviction-delay-seconds>");
			xml.append("<max-size policy=\"").append(m.getMaxSizeConfig().getMaxSizePolicy()).append("\">").append(m.getMaxSizeConfig().getSize()).append("</max-size>");
			xml.append("<time-to-live-seconds>").append(m.getTimeToLiveSeconds()).append("</time-to-live-seconds>");
			xml.append("<max-idle-seconds>").append(m.getMaxIdleSeconds()).append("</max-idle-seconds>");
			xml.append("<cache-value>").append(m.isCacheValue()).append("</cache-value>");
			xml.append("<read-backup-data>").append(m.isReadBackupData()).append("</read-backup-data>");
			xml.append("<merge-policy>").append(m.getMergePolicy()).append("</merge-policy>");
			
			if(m.getMapStoreConfig() != null) {
				final MapStoreConfig s = m.getMapStoreConfig();
				xml.append("<map-store enabled=\"").append(s.isEnabled()).append("\">");
				xml.append("<class-name>").append(s.getClassName()).append("</class-name>");
				xml.append("<write-delay-seconds>").append(s.getWriteDelaySeconds()).append("</write-delay-seconds>");
				appendProperties(xml, s.getProperties());
				xml.append("</map-store>");
			}
			if(m.getNearCacheConfig() != null) {
				final NearCacheConfig n = m.getNearCacheConfig();
				xml.append("<near-cache>");
				xml.append("<time-to-live-seconds>").append(n.getTimeToLiveSeconds()).append("</time-to-live-seconds>");
				xml.append("<max-idle-seconds>").append(n.getMaxIdleSeconds()).append("</max-idle-seconds>");
				xml.append("<max-size>").append(n.getMaxSize()).append("</max-size>");
				xml.append("<eviction-policy>").append(n.getEvictionPolicy()).append("</eviction-policy>");
				xml.append("<invalidate-on-change>").append(n.isInvalidateOnChange()).append("</invalidate-on-change>");
				xml.append("</near-cache>");
			}
			xml.append("</map>");
		}
		
		final Collection<MergePolicyConfig> merges = config.getMergePolicyConfigs().values();
		xml.append("<merge-policies>");
		for (MergePolicyConfig mp : merges) {
			xml.append("<map-merge-policy name=\"").append(mp.getName()).append("\">");
			final String clazz = mp.getImplementation() != null ? mp.getImplementation().getClass().getName() : mp.getClassName();
			xml.append("<class-name>").append(clazz).append("</class-name>");
			xml.append("</map-merge-policy>");
		}
		xml.append("</merge-policies>");
		
		xml.append("</hazelcast>");
		return format(xml.toString(), 5);
	}
	
	private String format(String input, int indent) {
		if(!formatted) {
			return input;
		}
		
	    try {
	        Source xmlInput = new StreamSource(new StringReader(input));
	        StringWriter stringWriter = new StringWriter();
	        StreamResult xmlOutput = new StreamResult(stringWriter);
	        TransformerFactory transformerFactory = TransformerFactory.newInstance();
	        transformerFactory.setAttribute("indent-number", indent);
	        Transformer transformer = transformerFactory.newTransformer();
    		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    		transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
	        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
	        transformer.transform(xmlInput, xmlOutput);
	        return xmlOutput.getWriter().toString();
	    } catch (Exception e) {
	        e.printStackTrace();
	        return input;
	    }
	}
	
	private void appendProperties(StringBuilder xml, Properties props) {
		if(!props.isEmpty()) {
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
