package com.hazelcast.map;

import com.hazelcast.core.AbstractIMapEvent;
import com.hazelcast.core.Member;

/**
 * Used for providing information about the lost partition for a map
 *
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 */
public class MapPartitionLostEvent extends AbstractIMapEvent {

    private static final long serialVersionUID = -7445734640964238109L;


    private final int partitionId;

    public MapPartitionLostEvent(Object source, Member member, int eventType, int partitionId) {
        super(source, member, eventType);
        this.partitionId = partitionId;
    }

    /**
     * Returns the partition id that has been lost for the given map
     *
     * @return the partition id that has been lost for the given map
     */
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String toString() {
        return "MapPartitionLostEvent{"
                + super.toString()
                + ", partitionId=" + partitionId
                + '}';
    }
}
