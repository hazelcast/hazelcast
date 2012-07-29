package com.hazelcast.jca;

/**
 * @deprecated Hopefully not required anymore with correct transaction handling
 */
@Deprecated
public class WasManagedConnectionImpl extends ManagedConnectionImpl {

	public WasManagedConnectionImpl(ResourceAdapterImpl resourceAdapter) {
		super(resourceAdapter);
	}

	@Override
	protected boolean isDeliverStartedEvent() {
		return false;
	}
	
	@Override
	protected boolean isDeliverClosed() {
		return true;
	}
	
	@Override
	protected boolean isDeliverCommitedEvent() {
		return false;
	}
	
	@Override
	protected boolean isDeliverRolledback() {
		return false;
	}
	
}
