package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.monitor.LocalGCStats;
import com.hazelcast.monitor.LocalMemoryStats;

import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getObject;

public class LocalMemoryStatsImpl implements LocalMemoryStats {

    private long creationTime;

    private long totalPhysical;

    private long freePhysical;

    private long maxNativeMemory;

    private long committedNativeMemory;

    private long usedNativeMemory;

    private long freeNativeMemory;

    private long maxHeap;

    private long committedHeap;

    private long usedHeap;

    private LocalGCStats gcStats;

    public LocalMemoryStatsImpl() {
    }

    public LocalMemoryStatsImpl(MemoryStats memoryStats) {
        setTotalPhysical(memoryStats.getTotalPhysical());
        setFreePhysical(memoryStats.getFreePhysical());
        setMaxNativeMemory(memoryStats.getMaxNativeMemory());
        setCommittedNativeMemory(memoryStats.getCommittedNativeMemory());
        setUsedNativeMemory(memoryStats.getUsedNativeMemory());
        setFreeNativeMemory(memoryStats.getFreeNativeMemory());
        setMaxHeap(memoryStats.getMaxHeap());
        setCommittedHeap(memoryStats.getCommittedHeap());
        setUsedHeap(memoryStats.getUsedHeap());
        setGcStats(new LocalGCStatsImpl(memoryStats.getGCStats()));
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
    public long getMaxNativeMemory() {
        return maxNativeMemory;
    }

    public void setMaxNativeMemory(long maxNativeMemory) {
        this.maxNativeMemory = maxNativeMemory;
    }

    @Override
    public long getCommittedNativeMemory() {
        return committedNativeMemory;
    }

    public void setCommittedNativeMemory(long allocated) {
        this.committedNativeMemory = allocated;
    }

    @Override
    public long getUsedNativeMemory() {
        return usedNativeMemory;
    }

    public void setUsedNativeMemory(long used) {
        this.usedNativeMemory = used;
    }

    @Override
    public long getFreeNativeMemory() {
        return freeNativeMemory;
    }

    public void setFreeNativeMemory(long freeNativeMemory) {
        this.freeNativeMemory = freeNativeMemory;
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
        final StringBuilder sb = new StringBuilder("LocalMemoryStats{");
        sb.append("totalPhysical=").append(totalPhysical);
        sb.append(", freePhysical=").append(freePhysical);
        sb.append(", maxNativeMemory=").append(maxNativeMemory);
        sb.append(", committedNativeMemory=").append(committedNativeMemory);
        sb.append(", usedNativeMemory=").append(usedNativeMemory);
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
        root.add("maxNativeMemory", maxNativeMemory);
        root.add("committedNativeMemory", committedNativeMemory);
        root.add("usedNativeMemory", usedNativeMemory);
        root.add("freeNativeMemory", freeNativeMemory);
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
        maxNativeMemory = getLong(json, "maxNativeMemory", -1L);
        committedNativeMemory = getLong(json, "committedNativeMemory", -1L);
        usedNativeMemory = getLong(json, "usedNativeMemory", -1L);
        freeNativeMemory = getLong(json, "freeNativeMemory", -1L);
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
        } else if (committedNativeMemory != that.committedNativeMemory) {
            return false;
        } else if (creationTime != that.creationTime) {
            return false;
        } else if (freePhysical != that.freePhysical) {
            return false;
        }  else if (maxNativeMemory != that.maxNativeMemory) {
            return false;
        } else if (totalPhysical != that.totalPhysical) {
            return false;
        } else if (usedNativeMemory != that.usedNativeMemory) {
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
        result = 31 * result + (int) (maxNativeMemory ^ (maxNativeMemory >>> 32));
        result = 31 * result + (int) (committedNativeMemory ^ (committedNativeMemory >>> 32));
        result = 31 * result + (int) (usedNativeMemory ^ (usedNativeMemory >>> 32));
        result = 31 * result + (int) (maxHeap ^ (maxHeap >>> 32));
        result = 31 * result + (int) (committedHeap ^ (committedHeap >>> 32));
        result = 31 * result + (int) (usedHeap ^ (usedHeap >>> 32));
        result = 31 * result + (gcStats != null ? gcStats.hashCode() : 0);
        return result;
    }

}
