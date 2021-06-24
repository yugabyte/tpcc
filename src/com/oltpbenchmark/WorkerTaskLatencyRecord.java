package com.oltpbenchmark;

/** Efficiently stores a record of (start time, latency) pairs. */
public class WorkerTaskLatencyRecord extends LatencyRecord {

    public WorkerTaskLatencyRecord(long startNs) {
        super(startNs);
    }

    public void addLatency(int transType, long startNs, long fetchWorkNs,
                           long keyingTimeNs, long executeWorkNs, long thinkTimeNs) {
        assert fetchWorkNs >= startNs;
        if (nextIndex == ALLOC_SIZE) {
            allocateChunk();
        }
        Sample[] chunk = (Sample[]) values.get(values.size() - 1);
        int fetchWorkLatencyUs = (int) ((fetchWorkNs - startNs + 500) / 1000);
        assert fetchWorkLatencyUs >= 0;

        int keyingLatencyUs = (int)((keyingTimeNs - fetchWorkNs + 500) / 1000);
        assert keyingLatencyUs >= 0;

        int aggregateExecuteUs = (int)((executeWorkNs - keyingTimeNs + 500) / 1000);
        assert aggregateExecuteUs >= 0;

        int thinkTimeUs = (int)((thinkTimeNs - executeWorkNs + 500) / 1000);
        assert thinkTimeUs >= 0;

        chunk[nextIndex] = new Sample(transType, startNs, fetchWorkLatencyUs,
                keyingLatencyUs, aggregateExecuteUs, thinkTimeUs);
        ++nextIndex;
    }

    protected LatencyRecord.Sample[] getNewLatencyRecord() {
        return new WorkerTaskLatencyRecord.Sample[ALLOC_SIZE];
    }

    /** Stores the start time and latencies (for a single worker task) for a single sample. Immutable. */
    public static final class Sample extends LatencyRecord.Sample {
        public final int fetchWorkUs;
        public final int keyingLatencyUs;
        public final int aggregateExecuteUs;
        public final int thinkTimeUs;

        public Sample(int tranType, long startNs, int fetchWorkUs,
                      int keyingLatencyUs, int aggregateExecuteUs, int thinkTimeUs) {
            super(tranType, startNs);
            this.fetchWorkUs = fetchWorkUs;
            this.keyingLatencyUs = keyingLatencyUs;
            this.aggregateExecuteUs = aggregateExecuteUs;
            this.thinkTimeUs = thinkTimeUs;
        }
    }
}
