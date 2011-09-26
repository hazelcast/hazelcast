/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.hibernate;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cache.StandardQueryCache;
import org.hibernate.stat.Statistics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.entity.DummyEntity;

public abstract class HibernateStatisticsTestSupport extends HibernateTestSupport {
	
	protected SessionFactory sf;
	protected Statistics stats;

	@Before
	public void postConstruct() {
		sf = createSessionFactory(getCacheProperties());
		stats = sf.getStatistics();
	}
	
	@After
	public void preDestroy() {
		if(sf != null) {
			sf.close();
		}
	}
	
	protected HazelcastInstance getHazelcastInstance() {
		return getHazelcastInstance(sf);
	}

	protected abstract Properties getCacheProperties();
	
	protected void insertDummyEntities(int count) {
		Session session = sf.openSession();
		Transaction tx = session.beginTransaction();
		try {
			for (int i = 0; i < count; i++) {
				session.save(new DummyEntity(new Long(i), "dummy:" + i, i * 123456d, new Date()));
			}
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
		} finally {
			session.close();
		}
	}
	
	@Test
	public void testEntity() {
		final HazelcastInstance hz = getHazelcastInstance();
		assertNotNull(hz);
		assertEquals(Hazelcast.getDefaultInstance(), hz);
		
		final int count = 100;
		insertDummyEntities(count);
		sleep(1);
		
		List<DummyEntity> list = new ArrayList<DummyEntity>(count);
		Session	session = sf.openSession();
		try {
			for (int i = 0; i < count; i++) {
				DummyEntity e = (DummyEntity) session.get(DummyEntity.class, new Long(i));
				session.evict(e);
				list.add(e);
			}
		} finally {
			session.close();
		}
		
		session = sf.openSession();
		Transaction tx = session.beginTransaction();
		try {
			for (DummyEntity dummy : list) {
				dummy.setDate(new Date());
				session.update(dummy);
			}
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
		} finally {
			session.close();
		}
		
		Map<?,?> cache = hz.getMap(DummyEntity.class.getName());
		assertEquals(count, stats.getEntityInsertCount());
		assertEquals(count * 2, stats.getSecondLevelCachePutCount());
		assertEquals(0, stats.getEntityLoadCount());
		assertEquals(count, stats.getSecondLevelCacheHitCount());
		assertEquals(0, stats.getSecondLevelCacheMissCount());
		assertEquals(count, cache.size());
		
		sf.getCache().evictEntityRegion(DummyEntity.class);
		assertEquals(0, cache.size());
		
		stats.logSummary();
	}
	
	@Test
	public void testQuery() {
		final HazelcastInstance hz = getHazelcastInstance();
		final int entityCount = 10; 
		final int queryCount = 5; 
		
		insertDummyEntities(entityCount);
		sleep(1);
		
		List<DummyEntity> list = null;
		for (int i = 0; i < queryCount; i++) {
			list = executeQuery();
			assertEquals(entityCount, list.size());
			sleep(1);
		}
		
		Session session = sf.openSession();
		Transaction tx = session.beginTransaction();
		try {
			for (DummyEntity dummy : list) {
				session.delete(dummy);
			}
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			e.printStackTrace();
		} finally {
			session.close();
		}
		
		assertEquals(1, stats.getQueryCachePutCount());
		assertEquals(1, stats.getQueryCacheMissCount());
		assertEquals(queryCount - 1, stats.getQueryCacheHitCount());
		assertEquals(1, stats.getQueryExecutionCount());
		assertEquals(entityCount, stats.getEntityInsertCount());
//		FIXME
//		HazelcastRegionFactory puts into L2 cache 2 times; 1 on insert, 1 on query execution 
//		assertEquals(entityCount, stats.getSecondLevelCachePutCount());
		assertEquals(entityCount, stats.getEntityLoadCount());
		assertEquals(entityCount, stats.getEntityDeleteCount());
		
		assertEquals(entityCount * (queryCount - 1), stats.getSecondLevelCacheHitCount());
		assertEquals(0, stats.getSecondLevelCacheMissCount());
		assertEquals(1, hz.getMap(StandardQueryCache.class.getName()).size());

		stats.logSummary();
	}
	
	private List<DummyEntity> executeQuery() {
		Session	session = sf.openSession();
		try {
			Query query = session.createQuery("from " + DummyEntity.class.getName());
			query.setCacheable(true);
			return query.list();
		} finally {
			session.close();
		}
	}
}
