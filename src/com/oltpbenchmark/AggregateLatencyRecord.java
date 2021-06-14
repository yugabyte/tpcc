package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Iterator;

/** Efficiently stores a record of (start time, latency) pairs. */
public class AggregateLatencyRecord implements Iterable<AggregateLatencyRecord.Sample> {
    /** Allocate space for 200 samples at a time */
    static final int ALLOC_SIZE = 200;

    /**
     * Contains (start time, latency, transactionType, workerid, phaseid) pentiplets
     * in microsecond form.
     */
    private final ArrayList<Sample[]> values = new ArrayList<>();
    private int nextIndex;

    public AggregateLatencyRecord(long startNs) {
        assert startNs > 0;
        allocateChunk();
    }

    public void addLatency(int transType, long startNs, long fetchWorkNs,
                           long keyingTimeNs, long executeWorkNs, long thinkTimeNs) {
        assert fetchWorkNs >= startNs;
        if (nextIndex == ALLOC_SIZE) {
            allocateChunk();
        }
        Sample[] chunk = values.get(values.size() - 1);
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

    private void allocateChunk() {
        assert (values.isEmpty() && nextIndex == 0)
                || nextIndex == ALLOC_SIZE;
        values.add(new Sample[ALLOC_SIZE]);
        nextIndex = 0;
    }

    /** Returns the number of recorded samples. */
    public int size() {
        // Samples stored in full chunks
        int samples = (values.size() - 1) * ALLOC_SIZE;

        // Samples stored in the last not full chunk
        samples += nextIndex;
        return samples;
    }

    /** Stores the start time and latency for a single sample. Immutable. */
    public static final class Sample implements Comparable<Sample> {
        public final int tranType;
        public final long startNs;
        public final int fetchWorkUs;
        public final int keyingLatencyUs;
        public final int aggregateExecuteUs;
        public final int thinkTimeUs;

        public Sample(int tranType, long startNs, int fetchWorkUs,
                      int keyingLatencyUs, int aggregateExecuteUs, int thinkTimeUs) {
            this.tranType = tranType;
            this.startNs = startNs;
            this.fetchWorkUs = fetchWorkUs;
            this.keyingLatencyUs = keyingLatencyUs;
            this.aggregateExecuteUs = aggregateExecuteUs;
            this.thinkTimeUs = thinkTimeUs;
        }

        @Override
        public int compareTo(Sample other) {
            long diff = this.startNs - other.startNs;

            // explicit comparison to avoid long to int overflow
            if (diff > 0)
                return 1;
            else if (diff < 0)
                return -1;
            else {
                return 0;
            }
        }
    }

    private final class AggregateLatencyRecordIterator implements Iterator<Sample> {
        private int chunkIndex = 0;
        private int subIndex = 0;

        @Override
        public boolean hasNext() {
            if (chunkIndex < values.size() - 1) {
                return true;
            }

            assert chunkIndex == values.size() - 1;
            if (subIndex < nextIndex) {
                return true;
            }

            assert chunkIndex == values.size() - 1 && subIndex == nextIndex;
            return false;
        }

        @Override
        public Sample next() {
            Sample[] chunk = values.get(chunkIndex);
            Sample s = chunk[subIndex];

            // Iterate in chunk, and wrap to next one
            ++subIndex;
            assert subIndex <= ALLOC_SIZE;
            if (subIndex == ALLOC_SIZE) {
                chunkIndex += 1;
                subIndex = 0;
            }
            return s;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not supported");
        }
    }

    public Iterator<Sample> iterator() {
        return new AggregateLatencyRecordIterator();
    }
}
