package com.hazelcast.internal.query.plan.physical;

import com.hazelcast.instance.Node;
import jnr.ffi.annotations.In;

import java.util.Map;

public class Edge {

    private Node fromNode;
    private Node toNode;
    private Map<String, Integer> fromParallelism;
    private Map<String, Integer> toParallelism;
}
