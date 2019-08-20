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

package org.apache.flink.runtime.state.heap.space;

/**
 * Chunk is a contiguous byteBuffer. or logically contiguous space .
 * for example: a Chunk is 1G space, maybe it's one big file, or multi 4M on/off-heap ByteBuffer
 */
public abstract class AbstractChunk implements Chunk {
	private final int chunkId;
	private final int capacity;

	AbstractChunk(int chunkId, int capacity) {
		this.chunkId = chunkId;
		this.capacity = capacity;
	}

	@Override
	public int getChunkId() {
		return this.chunkId;
	}

	@Override
	public int getChunkCapacity() {
		return this.capacity;
	}
}
