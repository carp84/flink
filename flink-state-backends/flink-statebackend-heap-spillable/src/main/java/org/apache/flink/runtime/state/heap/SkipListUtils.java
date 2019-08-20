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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.util.Preconditions;

import java.nio.ByteBuffer;

/**
 * Utilities for skip list.
 */
@SuppressWarnings("WeakerAccess")
public class SkipListUtils {
	static final long NIL_NODE = -1;
	static final long HEAD_NODE = -2;
	static final long NIL_VALUE_POINTER = -1;
	static final int MAX_LEVEL = 255;
	static final int DEFAULT_LEVEL = 32;
	static final int BYTE_MASK = 0xFF;

	/**
	 * Key space schema.
	 * - key meta
	 * -- int: level & status
	 * --   byte 0: level of node in skip list
	 * --   byte 1: node status
	 * --   byte 2: preserve
	 * --   byte 3: preserve
	 * -- int: length of key
	 * -- long: pointer to the newest value
	 * -- long: pointer to next node on level 0
	 * -- long[]: array of pointers to next node on different levels excluding level 0
	 * -- long[]: array of pointers to previous node on different levels excluding level 0
	 * - byte[]: data of key
	 */
	static final int KEY_META_OFFSET = 0;
	static final int KEY_LEN_OFFSET = KEY_META_OFFSET + Integer.BYTES;
	static final int VALUE_POINTER_OFFSET = KEY_LEN_OFFSET + Integer.BYTES;
	static final int NEXT_KEY_POINTER_OFFSET = VALUE_POINTER_OFFSET + Long.BYTES;
	static final int LEVEL_INDEX_OFFSET = NEXT_KEY_POINTER_OFFSET + Long.BYTES;


	/**
	 * Pre-compute the offset of index for different levels to dismiss the duplicated
	 * computation at runtime.
	 */
	private static final int[] INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY = new int[MAX_LEVEL + 1];

	/**
	 * Pre-compute the length of key meta for different levels to dismiss the duplicated
	 * computation at runtime.
	 */
	private static final int[] KEY_META_LEN_BY_LEVEL_ARRAY = new int[MAX_LEVEL + 1];

	static {
		for (int i = 1; i < INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY.length; i++) {
			INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[i] = LEVEL_INDEX_OFFSET + (i - 1) * Long.BYTES;
		}

		for (int i = 0; i < KEY_META_LEN_BY_LEVEL_ARRAY.length; i++) {
			KEY_META_LEN_BY_LEVEL_ARRAY[i] = LEVEL_INDEX_OFFSET + 2 * i * Long.BYTES;
		}
	}

	/**
	 * Returns the level of the node.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 */
	public static int getLevel(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toInt(byteBuffer, offset + KEY_META_OFFSET) & BYTE_MASK;
	}

	/**
	 * Returns the status of the node.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 */
	public static byte getNodeStatus(ByteBuffer byteBuffer, int offset) {
		return (byte) ((ByteBufferUtils.toInt(byteBuffer, offset + KEY_META_OFFSET) >>> 8) & BYTE_MASK);
	}

	/**
	 * Puts the level and status to the key space.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 * @param level the level.
	 * @param status the status.
	 */
	public static void putLevelAndNodeStatus(ByteBuffer byteBuffer, int offset, int level, byte status) {
		int data = ((status & BYTE_MASK) << 8) | level;
		ByteBufferUtils.putInt(byteBuffer, offset + SkipListUtils.KEY_META_OFFSET, data);
	}

	/**
	 * Returns the length of the key.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 */
	public static int getKeyLen(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toInt(byteBuffer, offset + KEY_LEN_OFFSET);
	}

	/**
	 * Puts the length of key to the key space.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 * @param keyLen length of key.
	 */
	public static void putKeyLen(ByteBuffer byteBuffer, int offset, int keyLen) {
		ByteBufferUtils.putInt(byteBuffer, offset + KEY_LEN_OFFSET, keyLen);
	}

	/**
	 * Returns the value pointer.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 */
	public static long getValuePointer(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toLong(byteBuffer, offset + VALUE_POINTER_OFFSET);
	}

	/**
	 * Puts the value pointer to key space.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 * @param valuePointer the value pointer.
	 */
	public static void putValuePointer(ByteBuffer byteBuffer, int offset, long valuePointer) {
		ByteBufferUtils.putLong(byteBuffer, offset + VALUE_POINTER_OFFSET, valuePointer);
	}

	/**
	 * Returns the next key pointer on level 0.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 */
	public static long getNextKeyPointer(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toLong(byteBuffer, offset + NEXT_KEY_POINTER_OFFSET);
	}

	/**
	 * Puts the next key pointer on level 0 to key space.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 * @param nextKeyPointer next key pointer on level 0.
	 */
	public static void putNextKeyPointer(ByteBuffer byteBuffer, int offset, long nextKeyPointer) {
		ByteBufferUtils.putLong(byteBuffer, offset + NEXT_KEY_POINTER_OFFSET, nextKeyPointer);
	}

	/**
	 * Returns next key pointer on the given index level.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 * @param level level of index.
	 */
	public static long getNextIndexNode(ByteBuffer byteBuffer, int offset, int level) {
		return ByteBufferUtils.toLong(byteBuffer, offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[level]);
	}

	/**
	 * Puts next key pointer on the given index level to key space.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 * @param level level of index.
	 * @param nextKeyPointer next key pointer on the given level.
	 */
	public static void putNextIndexNode(ByteBuffer byteBuffer, int offset, int level, long nextKeyPointer) {
		ByteBufferUtils.putLong(byteBuffer, offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[level], nextKeyPointer);
	}

	/**
	 * Returns previous key pointer on the given index level.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 * @param level level of index.
	 */
	public static long getPrevIndexNode(ByteBuffer byteBuffer, int offset, int totalLevel, int level) {
		int of = offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[totalLevel] + level * Long.BYTES;
		return ByteBufferUtils.toLong(byteBuffer, of);
	}

	/**
	 * Puts previous key pointer on the given index level to key space.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in the byte buffer.
	 * @param totalLevel top level of the key.
	 * @param level level of index.
	 * @param prevKeyPointer previous key pointer on the given level.
	 */
	public static void putPrevIndexNode(
		ByteBuffer byteBuffer, int offset, int totalLevel, int level, long prevKeyPointer) {
		int of = offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[totalLevel] + level * Long.BYTES;
		ByteBufferUtils.putLong(byteBuffer, of, prevKeyPointer);
	}

	/**
	 * Returns the length of key meta with the given level.
	 *
	 * @param level level of the key.
	 */
	public static int getKeyMetaLen(int level) {
		Preconditions.checkArgument(level >= 0 && level < KEY_META_LEN_BY_LEVEL_ARRAY.length,
			"level " + level + " out of range [0, " + KEY_META_LEN_BY_LEVEL_ARRAY.length + ")");
		return KEY_META_LEN_BY_LEVEL_ARRAY[level];
	}

	/**
	 * Returns the offset of key data in the key space.
	 *
	 * @param level level of the key.
	 */
	public static int getKeyDataOffset(int level) {
		return SkipListUtils.getKeyMetaLen(level);
	}

	/**
	 * Puts the key data into key space.
	 *
	 * @param byteBuffer byte buffer for key space.
	 * @param offset offset of key space in byte buffer.
	 * @param keyByteBuffer byte buffer for key data.
	 * @param keyOffset offset of key data in byte buffer.
	 * @param keyLen length of key data.
	 * @param level level of the key.
	 */
	public static void putKeyData(
		ByteBuffer byteBuffer, int offset, ByteBuffer keyByteBuffer, int keyOffset, int keyLen, int level) {
		ByteBufferUtils.copyFromBufferToBuffer(keyByteBuffer, byteBuffer, keyOffset,
			offset + getKeyDataOffset(level), keyLen);
	}

	/**
	 * Value space schema.
	 * - value meta
	 * -- int: version of this value to support copy on write
	 * -- long: pointer to the key space
	 * -- long: pointer to next older value
	 * -- int: length of data
	 * - byte[] data of value
	 */
	static final int VALUE_META_OFFSET = 0;
	static final int VALUE_VERSION_OFFSET = VALUE_META_OFFSET;
	static final int KEY_POINTER_OFFSET = VALUE_VERSION_OFFSET + Integer.BYTES;
	static final int NEXT_VALUE_POINTER_OFFSET = KEY_POINTER_OFFSET + Long.BYTES;
	static final int VALUE_LEN_OFFSET = NEXT_VALUE_POINTER_OFFSET + Long.BYTES;
	static final int VALUE_DATA_OFFSET = VALUE_LEN_OFFSET + Integer.BYTES;

	/**
	 * Returns the version of value.
	 *
	 * @param byteBuffer byte buffer for value space.
	 * @param offset offset of value space in byte buffer.
	 */
	public static int getValueVersion(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toInt(byteBuffer, offset + VALUE_VERSION_OFFSET);
	}

	/**
	 * Puts the version of value to value space.
	 *
	 * @param byteBuffer byte buffer for value space.
	 * @param offset offset of value space in byte buffer.
	 * @param version version of value.
	 */
	public static void putValueVersion(ByteBuffer byteBuffer, int offset, int version) {
		ByteBufferUtils.putInt(byteBuffer, offset + VALUE_VERSION_OFFSET, version);
	}

	/**
	 * Return the pointer to key space.
	 *
	 * @param byteBuffer byte buffer for value space.
	 * @param offset offset of value space in byte buffer.
	 */
	public static long getKeyPointer(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toLong(byteBuffer, offset + KEY_POINTER_OFFSET);
	}

	/**
	 * Puts the pointer of key space.
	 *
	 * @param byteBuffer byte buffer for value space.
	 * @param offset offset of value space in byte buffer.
	 * @param keyPointer pointer to key space.
	 */
	public static void putKeyPointer(ByteBuffer byteBuffer, int offset, long keyPointer) {
		ByteBufferUtils.putLong(byteBuffer, offset + KEY_POINTER_OFFSET, keyPointer);
	}

	/**
	 * Return the pointer to next value space.
	 *
	 * @param byteBuffer byte buffer for value space.
	 * @param offset offset of value space in byte buffer.
	 */
	public static long getNextValuePointer(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toLong(byteBuffer, offset + NEXT_VALUE_POINTER_OFFSET);
	}

	/**
	 * Puts the pointer of next value space.
	 *
	 * @param byteBuffer byte buffer for value space.
	 * @param offset offset of value space in byte buffer.
	 * @param nextValuePointer pointer to next value space.
	 */
	public static void putNextValuePointer(ByteBuffer byteBuffer, int offset, long nextValuePointer) {
		ByteBufferUtils.putLong(byteBuffer, offset + NEXT_VALUE_POINTER_OFFSET, nextValuePointer);
	}

	/**
	 * Return the length of value data.
	 *
	 * @param byteBuffer byte buffer for value space.
	 * @param offset offset of value space in byte buffer.
	 */
	public static int getValueLen(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toInt(byteBuffer, offset + VALUE_LEN_OFFSET);
	}

	/**
	 * Puts the length of value data.
	 *
	 * @param byteBuffer byte buffer for value space.
	 * @param offset offset of value space in byte buffer.
	 * @param valueLen length of value data.
	 */
	public static void putValueLen(ByteBuffer byteBuffer, int offset, int valueLen) {
		ByteBufferUtils.putInt(byteBuffer, offset + VALUE_LEN_OFFSET, valueLen);
	}

	/**
	 * Returns the length of value meta.
	 */
	public static int getValueMetaLen() {
		return VALUE_DATA_OFFSET;
	}

	/**
	 * Puts the value data into value space.
	 *
	 * @param byteBuffer byte buffer for value space.
	 * @param offset offset of value space in byte buffer.
	 * @param value value data.
	 */
	public static void putValueData(ByteBuffer byteBuffer, int offset, byte[] value) {
		ByteBufferUtils.copyFromArrayToBuffer(byteBuffer, offset + getValueMetaLen(), value, 0, value.length);
	}

	/**
	 * Status of the node.
	 */
	public enum NodeStatus {

		PUT((byte) 0), REMOVE((byte) 1);

		private byte value;

		NodeStatus(byte value) {
			this.value = value;
		}

		public byte getValue() {
			return value;
		}
	}
}
