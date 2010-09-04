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

package com.hazelcast.impl.management;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;

public abstract class ThreadDumpGenerator {
	
	private static final String THREAD_DUMP_15_CNAME = "ThreadDumpGeneratorImpl_15";
	private static final String THREAD_DUMP_16_CNAME = "ThreadDumpGeneratorImpl_16";
	private static final Class[] TYPE = new Class[]{ThreadMXBean.class};
	
	public static ThreadDumpGenerator newInstance() throws Exception {
		return newInstance(ManagementFactory.getThreadMXBean());
	}
	
	public static ThreadDumpGenerator newInstance(ThreadMXBean bean) throws Exception {
		String v = System.getProperty("java.specification.version");
		String cname = null;
		if("1.6".equals(v)) {
			cname = THREAD_DUMP_16_CNAME;
		}
		else {
			cname = THREAD_DUMP_15_CNAME;
		}
		
		String pkg = ThreadDumpGenerator.class.getPackage().getName();
		String className = pkg + "." + cname;
		Class clazz = ThreadDumpGenerator.class.getClassLoader().loadClass(className);
		Constructor<ThreadDumpGenerator> cons = clazz.getConstructor(TYPE);
		return cons.newInstance(bean);
	}
	
	protected final ThreadMXBean threadMxBean;
	
	ThreadDumpGenerator(ThreadMXBean bean) {
		super();
		this.threadMxBean = bean;
	}
	
	public final String dumpAllThreads() {
		StringBuilder s = new StringBuilder();
		s.append("Full thread dump ");
		return dump(getAllThreads(), s);
	}
	
	public final String dumpDeadlocks() {
		StringBuilder s = new StringBuilder();
		s.append("Deadlocked thread dump ");
		return dump(findDeadlockedThreads(), s);
	}
	
	private String dump(ThreadInfo[] infos, StringBuilder s) {
		header(s);
		appendThreadInfos(infos, s);
		return s.toString();
	}
	
	protected ThreadInfo[] getAllThreads() {
		return getThreads(threadMxBean.getAllThreadIds());
	}
	
	protected ThreadInfo[] findDeadlockedThreads() {
		return getThreads(threadMxBean.findMonitorDeadlockedThreads());
	}
	
	private void header(StringBuilder s) {
		s.append(System.getProperty("java.vm.name"));
		s.append(" (");
		s.append(System.getProperty("java.vm.version"));
		s.append(" ");
		s.append(System.getProperty("java.vm.info"));
		s.append("):");
		s.append("\n\n");
	}

	private void appendThreadInfos(ThreadInfo[] infos, StringBuilder s) {
		if(infos == null || infos.length == 0) return;
		for (int i = 0; i < infos.length; i++) {
			ThreadInfo info = infos[i];
			appendThreadInfo(info, s);
		}
	}
	
	protected abstract void appendThreadInfo(ThreadInfo info, StringBuilder sb);
	
	protected ThreadInfo[] getThreads(long[] tids) {
		if(tids == null || tids.length == 0) return null;
		return threadMxBean.getThreadInfo(tids, Integer.MAX_VALUE);
	}
}
