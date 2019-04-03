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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.StateTtlConfig.TtlTimeCharacteristic;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

/**
 * Provides time to TTL logic to judge about state expiration.
 */
public class TtlTimeProvider {
	public static final TtlTimeProvider DEFAULT = new TtlTimeProvider(TtlTimeCharacteristic.ProcessingTime);

	private final TtlTimeCharacteristic ttlTimeCharacteristic;
	private AbstractKeyedStateBackend keyedStateBackend;

	public TtlTimeProvider(TtlTimeCharacteristic ttlTimeCharacteristic) {
		this.ttlTimeCharacteristic = ttlTimeCharacteristic;
	}

	public long currentTimestamp() {
		switch (ttlTimeCharacteristic) {
			case ProcessingTime:
				return System.currentTimeMillis();
			case IngestionTime:
			case EventTime:
				Preconditions.checkNotNull(this.keyedStateBackend,
					"Cannot get timestamp before keyed backend is set");
				return keyedStateBackend.getCurrentTimestamp();
			default:
				throw new IllegalStateException("Unknown ttl time characteristic: " + ttlTimeCharacteristic);
		}
	}

	public void setKeyedStateBackend(@Nonnull AbstractKeyedStateBackend keyedStateBackend) {
		this.keyedStateBackend = keyedStateBackend;
	}

	public TtlTimeCharacteristic getTtlTimeCharacteristic() {
		return this.ttlTimeCharacteristic;
	}
}
