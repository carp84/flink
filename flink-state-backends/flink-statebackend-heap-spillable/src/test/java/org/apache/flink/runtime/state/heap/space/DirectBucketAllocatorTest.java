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

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.flink.runtime.state.heap.space.Constants.NO_SPACE;

/**
 * Tests for {@link DirectBucketAllocator}.
 */
public class DirectBucketAllocatorTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testNormal() throws Exception {
		Field filedUsedOffset2Length = DirectBucketAllocator.class.getDeclaredField("usedOffset2Length");
		Field filedFreeOffset2Length = DirectBucketAllocator.class.getDeclaredField("freeOffset2Length");
		Field filedCapacity = DirectBucketAllocator.class.getDeclaredField("capacity");
		Field filedOffset = DirectBucketAllocator.class.getDeclaredField("offset");
		filedUsedOffset2Length.setAccessible(true);
		filedFreeOffset2Length.setAccessible(true);
		filedCapacity.setAccessible(true);
		filedOffset.setAccessible(true);

		DirectBucketAllocator directBucketAllocator = new DirectBucketAllocator(100);
		SortedMap<Integer, Integer> usedOffset2Length =
			(TreeMap<Integer, Integer>) (filedUsedOffset2Length.get(directBucketAllocator));
		SortedMap<Integer, Integer> freeOffset2Length =
			(TreeMap<Integer, Integer>) (filedFreeOffset2Length.get(directBucketAllocator));
		int capacity = (int) (filedCapacity.get(directBucketAllocator));
		int offset = (int) (filedOffset.get(directBucketAllocator));

		Assert.assertEquals(0, usedOffset2Length.size());
		Assert.assertEquals(0, freeOffset2Length.size());
		Assert.assertEquals(100, capacity);
		Assert.assertEquals(0, offset);
		Assert.assertEquals("DirectBucketAllocator used=0;free=0;left=100", directBucketAllocator.toString());

		//allocate 11
		int offset1 = directBucketAllocator.allocate(11);
		Assert.assertEquals(0, offset1);
		Assert.assertEquals(11, usedOffset2Length.get(0).intValue());
		Assert.assertEquals(1, usedOffset2Length.size());
		Assert.assertEquals(0, freeOffset2Length.size());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(11, offset);
		Assert.assertEquals("DirectBucketAllocator used=11;free=0;left=89", directBucketAllocator.toString());

		//allocate 12
		int offset2 = directBucketAllocator.allocate(12);
		Assert.assertEquals(11, offset2);
		Assert.assertEquals(11, usedOffset2Length.get(0).intValue());
		Assert.assertEquals(12, usedOffset2Length.get(11).intValue());
		Assert.assertEquals(2, usedOffset2Length.size());
		Assert.assertEquals(0, freeOffset2Length.size());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(23, offset);
		Assert.assertEquals("DirectBucketAllocator used=23;free=0;left=77", directBucketAllocator.toString());

		//allocate 11
		int offset3 = directBucketAllocator.allocate(11);
		Assert.assertEquals(23, offset3);
		Assert.assertEquals(11, usedOffset2Length.get(0).intValue());
		Assert.assertEquals(12, usedOffset2Length.get(11).intValue());
		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(3, usedOffset2Length.size());
		Assert.assertEquals(0, freeOffset2Length.size());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(34, offset);
		Assert.assertEquals("DirectBucketAllocator used=34;free=0;left=66", directBucketAllocator.toString());

		//free offset1
		directBucketAllocator.free(offset1);
		Assert.assertEquals(12, usedOffset2Length.get(11).intValue());
		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(11, freeOffset2Length.get(0).intValue());
		Assert.assertEquals(2, usedOffset2Length.size());
		Assert.assertEquals(1, freeOffset2Length.size());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(34, offset);
		Assert.assertEquals("DirectBucketAllocator used=23;free=11;left=66", directBucketAllocator.toString());

		//
		int offset4 = directBucketAllocator.allocate(13);
		Assert.assertEquals(34, offset4);
		Assert.assertEquals(12, usedOffset2Length.get(11).intValue());
		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(13, usedOffset2Length.get(34).intValue());
		Assert.assertEquals(11, freeOffset2Length.get(0).intValue());
		Assert.assertEquals(3, usedOffset2Length.size());
		Assert.assertEquals(1, freeOffset2Length.size());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(47, offset);
		Assert.assertEquals("DirectBucketAllocator used=36;free=11;left=53", directBucketAllocator.toString());

		//allocate 11 again, will reuse the free space which have the same size
		offset1 = directBucketAllocator.allocate(11);
		Assert.assertEquals(0, offset1);
		Assert.assertEquals(11, usedOffset2Length.get(0).intValue());
		Assert.assertEquals(12, usedOffset2Length.get(11).intValue());
		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(13, usedOffset2Length.get(34).intValue());
		Assert.assertEquals(4, usedOffset2Length.size());
		Assert.assertEquals(0, freeOffset2Length.size());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(47, offset);
		Assert.assertEquals("DirectBucketAllocator used=47;free=0;left=53", directBucketAllocator.toString());

		int offset5 = directBucketAllocator.allocate(14);
		Assert.assertEquals(47, offset5);
		Assert.assertEquals(11, usedOffset2Length.get(0).intValue());
		Assert.assertEquals(12, usedOffset2Length.get(11).intValue());
		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(13, usedOffset2Length.get(34).intValue());
		Assert.assertEquals(14, usedOffset2Length.get(47).intValue());
		Assert.assertEquals(5, usedOffset2Length.size());
		Assert.assertEquals(0, freeOffset2Length.size());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(61, offset);
		Assert.assertEquals("DirectBucketAllocator used=61;free=0;left=39", directBucketAllocator.toString());

		int offset6 = directBucketAllocator.allocate(15);
		Assert.assertEquals(61, offset6);
		Assert.assertEquals(11, usedOffset2Length.get(0).intValue());
		Assert.assertEquals(12, usedOffset2Length.get(11).intValue());
		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(13, usedOffset2Length.get(34).intValue());
		Assert.assertEquals(14, usedOffset2Length.get(47).intValue());
		Assert.assertEquals(15, usedOffset2Length.get(61).intValue());
		Assert.assertEquals(6, usedOffset2Length.size());
		Assert.assertEquals(0, freeOffset2Length.size());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(76, offset);
		Assert.assertEquals("DirectBucketAllocator used=76;free=0;left=24", directBucketAllocator.toString());

		int offset7 = directBucketAllocator.allocate(16);
		Assert.assertEquals(76, offset7);
		Assert.assertEquals(11, usedOffset2Length.get(0).intValue());
		Assert.assertEquals(12, usedOffset2Length.get(11).intValue());
		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(13, usedOffset2Length.get(34).intValue());
		Assert.assertEquals(14, usedOffset2Length.get(47).intValue());
		Assert.assertEquals(15, usedOffset2Length.get(61).intValue());
		Assert.assertEquals(16, usedOffset2Length.get(76).intValue());
		Assert.assertEquals(7, usedOffset2Length.size());
		Assert.assertEquals(0, freeOffset2Length.size());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(92, offset);
		Assert.assertEquals("DirectBucketAllocator used=92;free=0;left=8", directBucketAllocator.toString());

		//no space
		int offset8 = directBucketAllocator.allocate(9);
		Assert.assertEquals(NO_SPACE, offset8);
		Assert.assertEquals("DirectBucketAllocator used=92;free=0;left=8", directBucketAllocator.toString());

		directBucketAllocator.free(offset1);
		directBucketAllocator.free(offset4);
		Assert.assertEquals(12, usedOffset2Length.get(11).intValue());
		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(14, usedOffset2Length.get(47).intValue());
		Assert.assertEquals(15, usedOffset2Length.get(61).intValue());
		Assert.assertEquals(16, usedOffset2Length.get(76).intValue());
		Assert.assertEquals(11, freeOffset2Length.get(0).intValue());
		Assert.assertEquals(13, freeOffset2Length.get(34).intValue());
		Assert.assertEquals(5, usedOffset2Length.size());
		Assert.assertEquals(2, freeOffset2Length.size());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(92, offset);
		Assert.assertEquals("DirectBucketAllocator used=68;free=24;left=8", directBucketAllocator.toString());

		//allocate 12, close use offset 34.
		int offset9 = directBucketAllocator.allocate(12);
		Assert.assertEquals(34, offset9);
		Assert.assertEquals(12, usedOffset2Length.get(11).intValue());
		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(12, usedOffset2Length.get(34).intValue());
		Assert.assertEquals(14, usedOffset2Length.get(47).intValue());
		Assert.assertEquals(15, usedOffset2Length.get(61).intValue());
		Assert.assertEquals(16, usedOffset2Length.get(76).intValue());
		Assert.assertEquals(11, freeOffset2Length.get(0).intValue());
		Assert.assertEquals(1, freeOffset2Length.get(46).intValue());
		Assert.assertEquals(6, usedOffset2Length.size());
		Assert.assertEquals(2, freeOffset2Length.size());
		Assert.assertEquals("DirectBucketAllocator used=80;free=12;left=8", directBucketAllocator.toString());

		directBucketAllocator.free(11);
		directBucketAllocator.free(47);
		directBucketAllocator.free(61);
		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(12, usedOffset2Length.get(34).intValue());
		Assert.assertEquals(16, usedOffset2Length.get(76).intValue());
		Assert.assertEquals(11, freeOffset2Length.get(0).intValue());
		Assert.assertEquals(12, freeOffset2Length.get(11).intValue());
		Assert.assertEquals(1, freeOffset2Length.get(46).intValue());
		Assert.assertEquals(14, freeOffset2Length.get(47).intValue());
		Assert.assertEquals(15, freeOffset2Length.get(61).intValue());
		Assert.assertEquals(3, usedOffset2Length.size());
		Assert.assertEquals(5, freeOffset2Length.size());
		Assert.assertEquals("DirectBucketAllocator used=39;free=53;left=8", directBucketAllocator.toString());
		//merge
		Assert.assertEquals(NO_SPACE, directBucketAllocator.allocate(31));

		Assert.assertEquals(11, usedOffset2Length.get(23).intValue());
		Assert.assertEquals(12, usedOffset2Length.get(34).intValue());
		Assert.assertEquals(16, usedOffset2Length.get(76).intValue());
		Assert.assertEquals(23, freeOffset2Length.get(0).intValue());
		Assert.assertEquals(30, freeOffset2Length.get(46).intValue());

		directBucketAllocator.free(23, 11);
		directBucketAllocator.free(34, 12);
		directBucketAllocator.free(76, 16);
		Assert.assertEquals("DirectBucketAllocator used=0;free=92;left=8", directBucketAllocator.toString());

		//merge
		Assert.assertEquals(0, directBucketAllocator.allocate(24));
		Assert.assertEquals(24, usedOffset2Length.get(0).intValue());
		Assert.assertEquals(68, freeOffset2Length.get(24).intValue());
		offset = (int) (filedOffset.get(directBucketAllocator));
		Assert.assertEquals(92, offset);
		Assert.assertEquals("DirectBucketAllocator used=24;free=68;left=8", directBucketAllocator.toString());
	}
}
