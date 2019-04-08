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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;

/**
 * The default {@link InternalKeyContext} implementation.
 *
 * @param <K> Type of the key.
 */
public class InternalKeyContextImpl<K> implements InternalKeyContext<K> {
	/** Range of key-groups for which this backend is responsible. */
	private final KeyGroupRange keyGroupRange;
	/** The number of key-groups aka max parallelism. */
	private final int numberOfKeyGroups;

	/** The currently active key. */
	private K currentKey;
	/** The key group of the currently active key. */
	private int currentKeyGroupIndex;
	/** {@link TypeSerializer} for the state backend key type. */
	private TypeSerializer<K> currentKeySerializer;

	public InternalKeyContextImpl(KeyGroupRange keyGroupRange, int numberOfKeyGroups, TypeSerializer<K> currentKeySerializer) {
		this.keyGroupRange = keyGroupRange;
		this.numberOfKeyGroups = numberOfKeyGroups;
		this.currentKeySerializer = currentKeySerializer;
	}

	@Override
	public K getCurrentKey() {
		return currentKey;
	}

	@Override
	public int getCurrentKeyGroupIndex() {
		return currentKeyGroupIndex;
	}

	@Override
	public int getNumberOfKeyGroups() {
		return numberOfKeyGroups;
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	@Override
	public TypeSerializer<K> getCurrentKeySerializer() {
		return currentKeySerializer;
	}

	@Override
	public void setCurrentKey(K currentKey) {
		this.currentKey = currentKey;
	}

	@Override
	public void setCurrentKeyGroupIndex(int currentKeyGroupIndex) {
		this.currentKeyGroupIndex = currentKeyGroupIndex;
	}

	@Override
	public void setCurrentKeySerializer(TypeSerializer<K> currentKeySerializer) {
		this.currentKeySerializer = currentKeySerializer;
	}
}
