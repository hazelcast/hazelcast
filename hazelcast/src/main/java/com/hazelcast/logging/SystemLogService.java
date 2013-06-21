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

import com.hazelcast.instance.Node;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class SystemLogService {

    public enum Level {
        NONE("none"),
        DEFAULT("default"),
        INFO("info"),
        TRACE("trace");

        private String value;

        public String getValue() {
            return value;
        }

        Level(String value) {
            this.value = value;
        }

        public static Level toLevel(String level) {
            if(level.equals("trace"))
                return Level.TRACE;
            else if(level.equals("default"))
                return Level.DEFAULT;
            else if(level.equals("info"))
                return Level.INFO;

            return Level.NONE;
        }
    }

    private final Queue<SystemLog> joinLogs = new LinkedBlockingQueue<SystemLog>(1000);

    private final Queue<SystemLog> connectionLogs = new LinkedBlockingQueue<SystemLog>(1000);

    private final Queue<SystemLog> partitionLogs = new LinkedBlockingQueue<SystemLog>(1000);

    private final Queue<SystemLog> nodeLogs = new LinkedBlockingQueue<SystemLog>(1000);

    private volatile Level currentLevel = Level.DEFAULT;

    private final Node node;

    private final boolean systemLogEnabled;

    public SystemLogService(Node node) {
        this.node = node;
        systemLogEnabled = node != null && node.groupProperties.SYSTEM_LOG_ENABLED.getBoolean();
    }

    public String getCurrentLevel() {
        return currentLevel.getValue();
    }

    public void setCurrentLevel(String level) {
        this.currentLevel = Level.toLevel(level);
    }

    public List<SystemLogRecord> getLogBundle() {
        ArrayList<SystemLogRecord> systemLogList = new ArrayList<SystemLogRecord>();
        String node = this.node.getThisAddress().getHost() + ":" + this.node.getThisAddress().getPort();
        for (SystemLog log : joinLogs) {
            systemLogList.add(new SystemLogRecord(0L, node, log.getDate(), log.toString(), log.getType().toString()));
        }
        for (SystemLog log : nodeLogs) {
            systemLogList.add(new SystemLogRecord(0L, node, log.getDate(), log.toString(), log.getType().toString()));
        }
        for (SystemLog log : connectionLogs) {
            systemLogList.add(new SystemLogRecord(0L, node, log.getDate(), log.toString(), log.getType().toString()));
        }
        for (SystemLog log : partitionLogs) {
            systemLogList.add(new SystemLogRecord(0L, node, log.getDate(), log.toString(), log.getType().toString()));
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
        sb.append(node.partitionService.toString());
        sb.append("\n");
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
}
