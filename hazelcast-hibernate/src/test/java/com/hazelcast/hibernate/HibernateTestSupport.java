package com.hazelcast.hibernate;

import java.net.URL;
import java.util.Properties;
import java.util.logging.Level;

import org.hibernate.SessionFactory;
import org.hibernate.cache.RegionFactory;
import org.hibernate.cache.impl.bridge.RegionFactoryCacheProviderBridge;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.SessionFactoryImplementor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.provider.HazelcastCacheProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

public abstract class HibernateTestSupport {
	
	private final ILogger logger = Logger.getLogger(getClass().getName());
	
	protected void sleep(int seconds) {
		try {
			logger.log(Level.INFO, "Waiting " + seconds + " seconds...");
			Thread.sleep(1000 * seconds);
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, "", e);
		}
	}
	
	protected static SessionFactory createSessionFactory(Properties props) {
		Configuration conf = new Configuration();
		URL xml = HibernateTestSupport.class.getClassLoader().getResource("test-hibernate.cfg.xml");
		conf.addProperties(props);
		conf.configure(xml);
		final SessionFactory sf = conf.buildSessionFactory();
		sf.getStatistics().setStatisticsEnabled(true);
		return sf;
	}
	
	protected static HazelcastInstance getHazelcastInstance(SessionFactory sf) {
		RegionFactory rf = ((SessionFactoryImplementor) sf).getSettings().getRegionFactory();
		if(rf instanceof HazelcastCacheRegionFactory) {
			return ((HazelcastCacheRegionFactory) rf).getHazelcastInstance();
		}
		else if(rf instanceof RegionFactoryCacheProviderBridge) {
			return ((HazelcastCacheProvider) ((RegionFactoryCacheProviderBridge) rf).getCacheProvider()).getHazelcastInstance();
		}
		return null;
	}
}
