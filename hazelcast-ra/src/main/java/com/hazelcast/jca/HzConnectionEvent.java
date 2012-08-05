package com.hazelcast.jca;

public enum HzConnectionEvent {
	FACTORY_INIT, CREATE, TX_START, TX_COMPLETE, CLEANUP, DESTROY,
}