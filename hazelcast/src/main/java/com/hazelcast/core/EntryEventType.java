/**
 * 
 */
package com.hazelcast.core;

public enum EntryEventType {
    ADDED(1),
    REMOVED(2),
    UPDATED(3),
    EVICTED(4);
    
    private int type;

    private EntryEventType(final int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
    
    public EntryEventType getByType(final int eventType) {
    	for(EntryEventType entryEventType : values()) {
    		if(entryEventType.type == eventType) {
    			return entryEventType;
    		}
    	}
    	return null;
    }
}