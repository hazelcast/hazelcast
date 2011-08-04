package com.hazelcast.spring;

import com.hazelcast.impl.Node;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.wan.WanReplicationEndpoint;

public class DummyWanReplication implements WanReplicationEndpoint {

	public void init(Node node, String groupName, String password, String... targets) {
	}

	public void recordUpdated(Record record) {
	}
}
