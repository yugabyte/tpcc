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
        int fetchWorkLatencyUs = toIntMicros(fetchWorkNs - startNs);
        int keyingLatencyUs = toIntMicros(keyingTimeNs - fetchWorkNs);
        int aggregateExecuteUs = toIntMicros(executeWorkNs - keyingTimeNs);
        int thinkTimeUs = toIntMicros(thinkTimeNs - executeWorkNs);

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
