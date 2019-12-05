/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * The settings regarding RocksDBs memory usage.
 */
public final class RocksDBMemoryConfiguration {

	/** Flag whether to use the managed memory budget for RocksDB. Null is not set. */
	@Nullable
	private Boolean useManagedMemory;

	/**The total memory for all RocksDB instances at this slot. Null is not set. */
	@Nullable
	private Long fixedMemoryPerSlot;

	/** The maximum fraction of the shared cache consumed by the write buffers. Null if not set.*/
	@Nullable
	private Double writeBufferRatio;

	/** The high priority pool ratio in the shared cache, used for index & filter blocks. Null if not set.*/
	@Nullable
	private Double highPriorityPoolRatio;

	// ------------------------------------------------------------------------

	public void setUseManagedMemory(boolean useManagedMemory) {
		this.useManagedMemory = useManagedMemory;
	}

	public void setFixedMemoryPerSlot(MemorySize fixedMemoryPerSlot) {
		if (fixedMemoryPerSlot != null) {
			final long bytes = fixedMemoryPerSlot.getBytes();
			Preconditions.checkArgument(bytes > 0, "Total memory per slot must be > 0, but is %s", bytes);
			this.fixedMemoryPerSlot = bytes;
		}
		else {
			this.fixedMemoryPerSlot = null;
		}
	}

	public void setFixedMemoryPerSlot(String totalMemoryPerSlotStr) {
		setFixedMemoryPerSlot(MemorySize.parse(totalMemoryPerSlotStr));
	}

	public void setWriteBufferRatio(double writeBufferRatio) {
		Preconditions.checkArgument(writeBufferRatio > 0 && writeBufferRatio < 1.0,
			"Write Buffer ratio %s must be in (0, 1)", writeBufferRatio);
		this.writeBufferRatio = writeBufferRatio;
	}

	public void setHighPriorityPoolRatio(double highPriorityPoolRatio) {
		Preconditions.checkArgument(highPriorityPoolRatio > 0 && highPriorityPoolRatio < 1.0,
			"High priority pool ratio %s must be in (0, 1)", highPriorityPoolRatio);
		this.highPriorityPoolRatio = highPriorityPoolRatio;
	}

	public boolean isUsingManagedMemory() {
		return useManagedMemory != null ? useManagedMemory : RocksDBOptions.USE_MANAGED_MEMORY.defaultValue();
	}

	public boolean isUsingFixedMemoryPerSlot() {
		return fixedMemoryPerSlot != null;
	}

	public long getFixedMemoryPerSlot() {
		if (fixedMemoryPerSlot == null) {
			throw new IllegalStateException("Fixed per-slot memory not configured.");
		}
		return fixedMemoryPerSlot;
	}

	public double getWriteBufferRatio() {
		return writeBufferRatio != null ? writeBufferRatio : RocksDBOptions.WRITE_BUFFER_RATIO.defaultValue();
	}

	public double getHighPriorityPoolRatio() {
		return highPriorityPoolRatio != null ? highPriorityPoolRatio : RocksDBOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue();
	}

	// ------------------------------------------------------------------------

	public void validate() {
		if (writeBufferRatio != null && highPriorityPoolRatio != null && writeBufferRatio + highPriorityPoolRatio > 1.0) {
			throw new IllegalArgumentException(String.format(
				"Invalid configuration: Sum of writeBufferRatio %s and highPriPoolRatio %s should be less than 1.0",
				writeBufferRatio, highPriorityPoolRatio));
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Derives a RocksDBMemoryConfiguration from another object and a configuration.
	 * The values set on the other object take precedence, and the values from the configuration are
	 * used if no values are set on the other config object.
	 */
	public static RocksDBMemoryConfiguration fromOtherAndConfiguration(
			RocksDBMemoryConfiguration other,
			Configuration config) {

		final RocksDBMemoryConfiguration newConfig = new RocksDBMemoryConfiguration();

		newConfig.fixedMemoryPerSlot = other.fixedMemoryPerSlot != null
				? other.fixedMemoryPerSlot
				: config.getOptional(RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE).orElse(null);

		newConfig.writeBufferRatio = other.writeBufferRatio != null
				? other.writeBufferRatio
				: config.getDouble(RocksDBOptions.WRITE_BUFFER_RATIO);

		newConfig.highPriorityPoolRatio = other.highPriorityPoolRatio != null
			? other.highPriorityPoolRatio
			: config.getDouble(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO);

		return newConfig;
	}
}
