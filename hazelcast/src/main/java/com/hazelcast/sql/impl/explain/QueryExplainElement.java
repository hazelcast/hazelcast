/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.explain;

/**
 * Query explain element.
 */
public class QueryExplainElement {
    /** ID of the node in the plan. */
    private final int id;

    /** Node type. */
    private final String type;

    /** Explain of the node. */
    private final String explain;

    /** Estimated number of rows. */
    private final double rows;

    /** Estimated CPU usage. */
    private final double cpu;

    /** Estimated IO. */
    private final double io;

    /** Level of nesting. */
    private final int level;

    public QueryExplainElement(int id, String type, String explain, double rows, double cpu, double io, int level) {
        this.id = id;
        this.type = type;
        this.explain = explain;
        this.rows = rows;
        this.cpu = cpu;
        this.io = io;
        this.level = level;
    }

    public int getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getExplain() {
        return explain;
    }

    public double getRows() {
        return rows;
    }

    public double getCpu() {
        return cpu;
    }

    public double getIo() {
        return io;
    }

    public int getLevel() {
        return level;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " {id=" + id + ", type='" + type + ", explain='" + explain
            + ", rows=" + rows + ", cpu=" + cpu + ", io=" + io + ", level=" + level + '}';
    }
}
