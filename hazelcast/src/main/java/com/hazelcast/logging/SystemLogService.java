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

package com.hazelcast.logging;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

public class SystemLogService {

    private final Queue<SystemLog> joinLogs = new LinkedBlockingQueue<SystemLog>(1000);
    private final Queue<SystemLog> connectionLogs = new LinkedBlockingQueue<SystemLog>(1000);
    private final Queue<SystemLog> partitionLogs = new LinkedBlockingQueue<SystemLog>(1000);
    private final Queue<SystemLog> nodeLogs = new LinkedBlockingQueue<SystemLog>(1000);
    private final Queue<SystemLog> warningLevelLogs = new LinkedBlockingQueue<SystemLog>(1000);
    private final boolean systemLogEnabled;

    public SystemLogService(boolean systemLogEnabled) {
        this.systemLogEnabled = systemLogEnabled;
    }

    public List<SystemLogRecord> getSystemWarnings() {
        ArrayList<SystemLog> systemLogList = new ArrayList<SystemLog>();
        ((LinkedBlockingQueue) warningLevelLogs).drainTo(systemLogList);
        ArrayList<SystemLogRecord> records = new ArrayList<SystemLogRecord>();
        for (SystemLog log : systemLogList) {
            records.add(new SystemLogRecord(log.getDate(), log.toString(), log.getType().toString()));
        }
        return records;
    }

    public List<SystemLogRecord> getLogBundle() {
        ArrayList<SystemLogRecord> systemLogList = new ArrayList<SystemLogRecord>();
        for (SystemLog log : joinLogs) {
            systemLogList.add(new SystemLogRecord(log.getDate(), log.toString(), log.getType().toString()));
        }
        for (SystemLog log : nodeLogs) {
            systemLogList.add(new SystemLogRecord(log.getDate(), log.toString(), log.getType().toString()));
        }
        for (SystemLog log : connectionLogs) {
            systemLogList.add(new SystemLogRecord(log.getDate(), log.toString(), log.getType().toString()));
        }
        for (SystemLog log : partitionLogs) {
            systemLogList.add(new SystemLogRecord(log.getDate(), log.toString(), log.getType().toString()));
        }
        return systemLogList;
    }

    public void shutdown() {
        connectionLogs.clear();
        nodeLogs.clear();
        joinLogs.clear();
        partitionLogs.clear();
    }

    public String dump() {
        StringBuilder sb = new StringBuilder();
        Set<SystemLog> sorted = new TreeSet<SystemLog>(new Comparator<SystemLog>() {
            public int compare(SystemLog o1, SystemLog o2) {
                long thisVal = o1.date;
                long anotherVal = o2.date;
                return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
            }
        });
        sorted.addAll(joinLogs);
        sorted.addAll(nodeLogs);
        sorted.addAll(connectionLogs);
        sorted.addAll(partitionLogs);
        for (SystemLog systemLog : sorted) {
            sb.append(systemLog.getType().toString());
            sb.append(" - ");
            sb.append(new Date(systemLog.getDate()).toString());
            sb.append(" - ");
            sb.append(systemLog.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

    public void logConnection(String str) {
        if (systemLogEnabled) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.CONNECTION);
            connectionLogs.offer(systemLog);
        }
    }

    public void logPartition(String str) {
        if (systemLogEnabled) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.PARTITION);
            partitionLogs.offer(systemLog);
        }
    }

    public void logNode(String str) {
        if (systemLogEnabled) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.NODE);
            nodeLogs.offer(systemLog);
        }
    }

    public void logJoin(String str) {
        if (systemLogEnabled) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.JOIN);
            joinLogs.offer(systemLog);
        }
    }

    public void logWarningConnection(String str) {
        if (systemLogEnabled) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.CONNECTION);
            connectionLogs.offer(systemLog);
            warningLevelLogs.offer(systemLog);
        }
    }

    public void logWarningPartition(String str) {
        if (systemLogEnabled) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.PARTITION);
            partitionLogs.offer(systemLog);
            warningLevelLogs.offer(systemLog);
        }
    }

    public void logWarningNode(String str) {
        if (systemLogEnabled) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.NODE);
            nodeLogs.offer(systemLog);
            warningLevelLogs.offer(systemLog);
        }
    }

    public void logWarningJoin(String str) {
        if (systemLogEnabled) {
            SystemObjectLog systemLog = new SystemObjectLog(str);
            systemLog.setType(SystemLog.Type.JOIN);
            joinLogs.offer(systemLog);
            warningLevelLogs.offer(systemLog);
        }
    }

}
