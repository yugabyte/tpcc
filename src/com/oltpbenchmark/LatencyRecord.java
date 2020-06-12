/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Iterator;

/** Efficiently stores a record of (start time, latency) pairs. */
public class LatencyRecord implements Iterable<LatencyRecord.Sample> {
	/** Allocate space for 500k samples at a time */
	static final int ALLOC_SIZE = 500000;

	/**
	 * Contains (start time, latency, transactionType, workerid, phaseid) pentiplets 
	 * in microsecond form. 
	 */
	private final ArrayList<Sample[]> values = new ArrayList<Sample[]>();
	private int nextIndex;

	public LatencyRecord(long startNs) {
		assert startNs > 0;
		allocateChunk();
	}

    public void addLatency(int transType, long startNs, long endNs, int workerId, int phaseId) {
		assert endNs >= startNs;
		if (nextIndex == ALLOC_SIZE) {
			allocateChunk();
		}
		Sample[] chunk = values.get(values.size() - 1);
		int latencyUs = (int) ((endNs - startNs + 500) / 1000);
		assert latencyUs >= 0;

		chunk[nextIndex] = new Sample(transType, startNs, latencyUs, workerId, phaseId);
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
		public long startNs;
		public final int latencyUs;
		public final int workerId;
		public final int phaseId;

        public Sample(int tranType, long startNs, int latencyUs, int workerId, int phaseId) {
			this.tranType = tranType;
			this.startNs = startNs;
			this.latencyUs = latencyUs;
			this.workerId = workerId;
			this.phaseId = phaseId;
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
				assert diff == 0;
				return 0;
			}
		}
	}

	private final class LatencyRecordIterator implements Iterator<Sample> {
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
		return new LatencyRecordIterator();
	}
}
