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

/**
 * ThreadDump JavaScript implementation
 * 
 * Underscore sign (_) marks functions as private.  
 */

importPackage(java.lang);
importPackage(java.util);
importPackage(java.lang.management);

var threadMxBean = ManagementFactory.getThreadMXBean();

function dumpAll() {
	var s = new StringBuilder();
	s.append("Full thread dump ");
	return _dump(_getAllThreads(), s);
}

function dumpDeadlocks() {
	var s = new StringBuilder();
	s.append("Deadlocked thread dump ");
	return _dump(_findDeadlockedThreads(), s);
}

function _dump(infos, s) {
	_header(s);
	_appendThreadInfos(infos, s);
	return s.toString();
}

function _getAllThreads() {
	if (threadMxBean.isObjectMonitorUsageSupported()
			&& threadMxBean.isSynchronizerUsageSupported()) {
		
		return threadMxBean.dumpAllThreads(true, true)
	} else {
		return _getThreads(threadMxBean.getAllThreadIds());
	}
}

function _findDeadlockedThreads() {
	var tids = null;
	var infos = null;
	
	if (threadMxBean.isSynchronizerUsageSupported()) {
		tids = threadMxBean.findDeadlockedThreads();
		if (tids == null) {
			return null;
		}

		return threadMxBean.getThreadInfo(tids, true, true);
		
	} else {
		return _getThreads(threadMxBean.findMonitorDeadlockedThreads());
	}
}

function _getThreads(tids) {
	return threadMxBean.getThreadInfo(tids, Integer.MAX_VALUE);
}

function _header(s) {
	s.append(System.getProperty("java.vm.name"));
	s.append(" (");
	s.append(System.getProperty("java.vm.version"));
	s.append(" ");
	s.append(System.getProperty("java.vm.info"));
	s.append("):");
	s.append("\n\n");
}

function _appendThreadInfos(infos, s) {
	if(infos == null || infos.length == 0) return;
	for (var i = 0; i < infos.length; i++) {
		var info = infos[i];
		_appendThreadInfo(info, s);
	}
}

function _appendThreadInfo(info, sb) {
    sb.append("\"" + info.getThreadName() + "\"" +
                                         " Id=" + info.getThreadId() + " " +
                                         info.getThreadState());
    if (info.getLockName() != null) {
        sb.append(" on " + info.getLockName());
    }
    if (info.getLockOwnerName() != null) {
        sb.append(" owned by \"" + info.getLockOwnerName() +
                  "\" Id=" + info.getLockOwnerId());
    }
    if (info.isSuspended()) {
        sb.append(" (suspended)");
    }
    if (info.isInNative()) {
        sb.append(" (in native)");
    }
    sb.append('\n');
    
    var stackTrace = info.getStackTrace();
    var i = 0;
    for (; i < stackTrace.length; i++) {
        var ste = stackTrace[i];
        sb.append("\tat " + ste.toString());
        sb.append('\n');
        if (i == 0 && info.getLockInfo() != null) {
            var ts = info.getThreadState();
            switch (ts) {
                case Thread.State.BLOCKED: 
                    sb.append("\t-  blocked on " + info.getLockInfo());
                    sb.append('\n');
                    break;
                case Thread.State.WAITING:
                    sb.append("\t-  waiting on " + info.getLockInfo());
                    sb.append('\n');
                    break;
                case Thread.State.TIMED_WAITING:
                    sb.append("\t-  waiting on " + info.getLockInfo());
                    sb.append('\n');
                    break;
                default:
            }
        }

        var mons = info.getLockedMonitors();
        for (var j = 0; j < mons.length; j++ ) {
            if (mons[j].getLockedStackDepth() == i) {
                sb.append("\t-  locked " + mons[j]);
                sb.append('\n');
            }
        }
   }
   if (i < stackTrace.length) {
       sb.append("\t...");
       sb.append('\n');
   }

   var locks = info.getLockedSynchronizers();
   if (locks.length > 0) {
       sb.append("\n\tNumber of locked synchronizers = " + locks.length);
       sb.append('\n');
       for (var k = 0; k < mons.length; k++ ) {
           sb.append("\t- " + locks[k]);
           sb.append('\n');
       }
   }
   sb.append('\n');
}