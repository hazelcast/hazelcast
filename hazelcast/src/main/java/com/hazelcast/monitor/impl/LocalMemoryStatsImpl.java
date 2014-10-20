package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.LocalGCStats;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getObject;

public class LocalMemoryStatsImpl implements LocalMemoryStats {

    private long creationTime;

    private long totalPhysical;

    private long freePhysical;

    private long maxOffHeap;

    private long committedOffHeap;

    private long usedOffHeap;

    private long maxHeap;

    private long committedHeap;

    private long usedHeap;

    private LocalGCStats gcStats;

    public LocalMemoryStatsImpl() {
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(totalPhysical);
        out.writeLong(freePhysical);
        out.writeLong(maxOffHeap);
        out.writeLong(committedOffHeap);
        out.writeLong(usedOffHeap);
        out.writeLong(maxHeap);
        out.writeLong(committedHeap);
        out.writeLong(usedHeap);
        if (gcStats == null) {
            gcStats = new LocalGCStatsImpl();
        }
        gcStats.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        totalPhysical = in.readLong();
        freePhysical = in.readLong();
        maxOffHeap = in.readLong();
        committedOffHeap = in.readLong();
        usedOffHeap = in.readLong();
        maxHeap = in.readLong();
        committedHeap = in.readLong();
        usedHeap = in.readLong();
        gcStats = new LocalGCStatsImpl();
        gcStats.readData(in);
    }

    @Override
    public long getTotalPhysical() {
        return totalPhysical;
    }

    public void setTotalPhysical(long totalPhysical) {
        this.totalPhysical = totalPhysical;
    }

    @Override
    public long getFreePhysical() {
        return freePhysical;
    }

    public void setFreePhysical(long freePhysical) {
        this.freePhysical = freePhysical;
    }

    @Override
    public long getMaxOffHeap() {
        return maxOffHeap;
    }

    public void setMaxOffHeap(long maxOffHeap) {
        this.maxOffHeap = maxOffHeap;
    }

    @Override
    public long getCommittedOffHeap() {
        return committedOffHeap;
    }

    public void setCommittedOffHeap(long allocated) {
        this.committedOffHeap = allocated;
    }

    @Override
    public long getUsedOffHeap() {
        return usedOffHeap;
    }

    public void setUsedOffHeap(long used) {
        this.usedOffHeap = used;
    }

    @Override
    public long getFreeOffHeap() {
        return maxOffHeap - usedOffHeap;
    }

    @Override
    public long getMaxHeap() {
        return maxHeap;
    }

    @Override
    public long getCommittedHeap() {
        return committedHeap;
    }

    @Override
    public long getUsedHeap() {
        return usedHeap;
    }

    public void setMaxHeap(long maxHeap) {
        this.maxHeap = maxHeap;
    }

    public void setCommittedHeap(long committedHeap) {
        this.committedHeap = committedHeap;
    }

    public void setUsedHeap(long usedHeap) {
        this.usedHeap = usedHeap;
    }

    @Override
    public long getFreeHeap() {
        return maxHeap - usedHeap;
    }

    @Override
    public LocalGCStats getGCStats() {
        return gcStats;
    }

    public void setGcStats(LocalGCStats gcStats) {
        this.gcStats = gcStats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SerializableMemoryStats{");
        sb.append("totalPhysical=").append(totalPhysical);
        sb.append(", freePhysical=").append(freePhysical);
        sb.append(", maxOffHeap=").append(maxOffHeap);
        sb.append(", committedOffHeap=").append(committedOffHeap);
        sb.append(", usedOffHeap=").append(usedOffHeap);
        sb.append(", maxHeap=").append(maxHeap);
        sb.append(", committedHeap=").append(committedHeap);
        sb.append(", usedHeap=").append(usedHeap);
        sb.append(", gcStats=").append(gcStats);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", creationTime);
        root.add("totalPhysical", totalPhysical);
        root.add("freePhysical", freePhysical);
        root.add("maxOffHeap", maxOffHeap);
        root.add("committedOffHeap", committedOffHeap);
        root.add("usedOffHeap", usedOffHeap);
        root.add("maxHeap", maxHeap);
        root.add("committedHeap", committedHeap);
        root.add("usedHeap", usedHeap);
        if (gcStats == null) {
            gcStats = new LocalGCStatsImpl();
        }
        root.add("gcStats", gcStats.toJson());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        totalPhysical = getLong(json, "totalPhysical", -1L);
        freePhysical = getLong(json, "freePhysical", -1L);
        maxOffHeap = getLong(json, "maxOffHeap", -1L);
        committedOffHeap = getLong(json, "committedOffHeap", -1L);
        usedOffHeap = getLong(json, "usedOffHeap", -1L);
        maxHeap = getLong(json, "maxHeap", -1L);
        committedHeap = getLong(json, "committedHeap", -1L);
        usedHeap = getLong(json, "usedHeap", -1L);
        gcStats = new LocalGCStatsImpl();
        if (json.get("gcStats") != null) {
            gcStats.fromJson(getObject(json, "gcStats"));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LocalMemoryStatsImpl that = (LocalMemoryStatsImpl) o;

        if (!heapStatsEquals(that)) {
            return false;
        } else if (committedOffHeap != that.committedOffHeap) {
            return false;
        } else if (creationTime != that.creationTime) {
            return false;
        } else if (freePhysical != that.freePhysical) {
            return false;
        }  else if (maxOffHeap != that.maxOffHeap) {
            return false;
        } else if (totalPhysical != that.totalPhysical) {
            return false;
        } else if (usedOffHeap != that.usedOffHeap) {
            return false;
        } else if (!that.getGCStats().equals(gcStats)) {
            return false;
        }

        return true;
    }

    /**
     * Extracted reduce cyclomatic complexity of {@link com.hazelcast.monitor.impl.LocalMemoryStatsImpl#equals(Object)} method
     */
    private boolean heapStatsEquals(LocalMemoryStatsImpl that) {
        if (committedHeap != that.committedHeap) {
            return false;
        } else if (maxHeap != that.maxHeap) {
            return false;
        }  else if (usedHeap != that.usedHeap) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (totalPhysical ^ (totalPhysical >>> 32));
        result = 31 * result + (int) (freePhysical ^ (freePhysical >>> 32));
        result = 31 * result + (int) (maxOffHeap ^ (maxOffHeap >>> 32));
        result = 31 * result + (int) (committedOffHeap ^ (committedOffHeap >>> 32));
        result = 31 * result + (int) (usedOffHeap ^ (usedOffHeap >>> 32));
        result = 31 * result + (int) (maxHeap ^ (maxHeap >>> 32));
        result = 31 * result + (int) (committedHeap ^ (committedHeap >>> 32));
        result = 31 * result + (int) (usedHeap ^ (usedHeap >>> 32));
        result = 31 * result + (gcStats != null ? gcStats.hashCode() : 0);
        return result;
    }
}
