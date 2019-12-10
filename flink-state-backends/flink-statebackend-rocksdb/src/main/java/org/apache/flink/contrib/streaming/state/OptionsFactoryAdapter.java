/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.util.ArrayList;

/**
 * A conversion from {@link RocksDBOptionsFactory} to {@link OptionsFactory}.
 *
 * <p>For backward compatibility, will be removed in one of the next versions.
 */
@SuppressWarnings("deprecation")
public class OptionsFactoryAdapter implements OptionsFactory {
	private static final long serialVersionUID = 1L;

	private final RocksDBOptionsFactory rocksDBOptionsFactory;

	OptionsFactoryAdapter(RocksDBOptionsFactory rocksDBOptionsFactory) {
		this.rocksDBOptionsFactory = rocksDBOptionsFactory;
	}

	@Override
	public DBOptions createDBOptions(DBOptions currentOptions) {
		return rocksDBOptionsFactory.createDBOptions(currentOptions, new ArrayList<>());
	}

	@Override
	public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
		return rocksDBOptionsFactory.createColumnOptions(currentOptions, new ArrayList<>());
	}

	@Override
	public int hashCode() {
		return rocksDBOptionsFactory.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other instanceof OptionsFactoryAdapter) {
			OptionsFactoryAdapter otherAdapter = (OptionsFactoryAdapter) other;
			return this.rocksDBOptionsFactory.equals(otherAdapter.rocksDBOptionsFactory);
		}
		return false;
	}
}
