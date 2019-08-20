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

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.state.heap.space.Constants.NO_SPACE;
import static org.apache.flink.runtime.state.heap.space.PowerTwoBucketAllocator.getBucketItemSizeFromIndex;

/**
 * Tests for {@link PowerTwoBucketAllocator}.
 */
public class PowerTwoBucketAllocatorTest {

	@Test
	public void testConstruct() {
		PowerTwoBucketAllocator power = new PowerTwoBucketAllocator(1024 * 1024 * 1024, 1024 * 1024);

		PowerTwoBucketAllocator.Bucket[] allBuckets = power.getAllBuckets();
		PowerTwoBucketAllocator.BucketSizeInfo[] bucketSizeInfo = power.getBucketSizeInfos();

		Assert.assertEquals(1024, allBuckets.length);
		Assert.assertEquals(16, bucketSizeInfo.length);

		int bucketSum = 0;
		for (int i = 0; i < bucketSizeInfo.length; i++) {
			Assert.assertEquals(i, bucketSizeInfo[i].sizeIndex());
			for (Object object : bucketSizeInfo[i].getBucketList().keySet()) {
				PowerTwoBucketAllocator.Bucket bucket = (PowerTwoBucketAllocator.Bucket) object;
				Assert.assertEquals(i, bucket.sizeIndex());
				Assert.assertTrue(bucket.isCompletelyFree());
				Assert.assertTrue(!bucket.isUninitiated());
				Assert.assertEquals(1 << (i + 5), bucket.getItemAllocationSize());
				Assert.assertEquals(1024 * 1024 / (1 << (i + 5)), bucket.freeCount());
				Assert.assertEquals(0, bucket.usedCount());
				Assert.assertEquals(1024 * 1024, bucket.getFreeBytes());
				Assert.assertEquals(0, bucket.getUsedBytes());
				bucketSum++;
			}
		}
		Assert.assertEquals(bucketSum, 0);
		Assert.assertEquals(1024, power.getCompleteFreeBucketQueue().size());
	}

	@Test
	public void testBucket() {
		doTestBucket(0, 1024);
		doTestBucket(32, 1024);
		doTestBucket(64, 1024);
		doTestBucket(128, 1024);
		doTestBucket(256, 1024);
		doTestBucket(512, 1024);
		doTestBucket(1024, 1024);
		doTestBucket(1024, 1024 * 1024);
	}

	private void doTestBucket(int baseOffset, int bucketCapacity) {
		PowerTwoBucketAllocator.Bucket bucket = new PowerTwoBucketAllocator.Bucket(baseOffset, bucketCapacity);

		//Uninitiated
		Assert.assertTrue(bucket.isUninitiated());
		Assert.assertEquals(baseOffset, bucket.getBaseOffset());
		Assert.assertEquals(0, bucket.usedCount());
		Assert.assertEquals(0, bucket.freeCount());
		Assert.assertEquals(0, bucket.getUsedBytes());
		Assert.assertEquals(0, bucket.getFreeBytes());

		//instantiate
		//32bytes
		int sizeIndex = 2;
		int itemSize = 1 << (sizeIndex + 5);
		bucket.reconfigure(sizeIndex);
		Assert.assertEquals(baseOffset, bucket.getBaseOffset());
		Assert.assertEquals(0, bucket.usedCount());
		Assert.assertEquals(bucketCapacity / itemSize, bucket.freeCount());
		Assert.assertEquals(0, bucket.getUsedBytes());
		Assert.assertEquals(itemSize, bucket.getItemAllocationSize());
		Assert.assertEquals(bucketCapacity, bucket.getFreeBytes());
		Assert.assertTrue(bucket.isCompletelyFree());
		Assert.assertTrue(bucket.hasFreeSpace());

		//first allocate
		int offset = bucket.allocate();
		Assert.assertEquals(baseOffset + bucketCapacity - itemSize, offset);
		Assert.assertEquals(1, bucket.usedCount());
		Assert.assertEquals(bucketCapacity / itemSize - 1, bucket.freeCount());

		//free allocate
		bucket.free(offset);
		Assert.assertEquals(0, bucket.usedCount());
		Assert.assertEquals(bucketCapacity / itemSize, bucket.freeCount());

		List<Integer> allOffset = new ArrayList<>();
		while (bucket.hasFreeSpace()) {
			offset = bucket.allocate();
			allOffset.add(offset);
			int totalAllocate = allOffset.size();
			Assert.assertEquals(baseOffset + bucketCapacity - totalAllocate * itemSize, offset);
			Assert.assertEquals(totalAllocate, bucket.usedCount());
			Assert.assertEquals(bucketCapacity / itemSize - totalAllocate, bucket.freeCount());
		}

		Assert.assertEquals(bucketCapacity / itemSize, allOffset.size());
		Assert.assertEquals(bucketCapacity / itemSize, (new HashSet<>(allOffset)).size());

		Collections.shuffle(allOffset);
		//disorder free offset
		for (Integer off : allOffset) {
			bucket.free(off);
		}

		allOffset = new ArrayList<>();
		while (bucket.hasFreeSpace()) {
			offset = bucket.allocate();
			allOffset.add(offset);
			int totalAllocate = allOffset.size();
			Assert.assertEquals(totalAllocate, bucket.usedCount());
			Assert.assertEquals(bucketCapacity / itemSize - totalAllocate, bucket.freeCount());
		}

		Assert.assertEquals(bucketCapacity / itemSize, allOffset.size());
		Assert.assertEquals(bucketCapacity / itemSize, (new HashSet<>(allOffset)).size());

	}

	@Test
	public void testGetBucketSizeIndex() {
		//index 0 means  32 (2 <<< 5)
		//index 1 means  64 (2 <<< 6)
		Assert.assertEquals(0, PowerTwoBucketAllocator.getBucketSizeInfoIndex(1));
		Assert.assertEquals(0, PowerTwoBucketAllocator.getBucketSizeInfoIndex(32));
		Assert.assertEquals(1, PowerTwoBucketAllocator.getBucketSizeInfoIndex(33));
		Assert.assertEquals(1, PowerTwoBucketAllocator.getBucketSizeInfoIndex(64));
		Assert.assertEquals(2, PowerTwoBucketAllocator.getBucketSizeInfoIndex(65));
		Assert.assertEquals(2, PowerTwoBucketAllocator.getBucketSizeInfoIndex(128));
		Assert.assertEquals(3, PowerTwoBucketAllocator.getBucketSizeInfoIndex(129));
		Assert.assertEquals(3, PowerTwoBucketAllocator.getBucketSizeInfoIndex(256));
		Assert.assertEquals(16, PowerTwoBucketAllocator.getBucketSizeInfoIndex(2 * 1024 * 1024));
		Assert.assertEquals(17, PowerTwoBucketAllocator.getBucketSizeInfoIndex(2 * 1024 * 1024 + 1));
		Assert.assertEquals(17, PowerTwoBucketAllocator.getBucketSizeInfoIndex(4 * 1024 * 1024));

		Assert.assertEquals(32, getBucketItemSizeFromIndex(0));
		Assert.assertEquals(64, getBucketItemSizeFromIndex(1));
		Assert.assertEquals(128, getBucketItemSizeFromIndex(2));
		Assert.assertEquals(4194304, getBucketItemSizeFromIndex(17));
	}

	@Test
	public void testBucketSizeInfo() {
		PowerTwoBucketAllocator power = new PowerTwoBucketAllocator(1024, 128);

		PowerTwoBucketAllocator.Bucket[] allBuckets = power.getAllBuckets();
		PowerTwoBucketAllocator.BucketSizeInfo[] bucketSizeInfo = power.getBucketSizeInfos();

		Assert.assertEquals(8, allBuckets.length);
		Assert.assertEquals(3, bucketSizeInfo.length);

		Assert.assertEquals(0, bucketSizeInfo[0].getBucketList().size());
		Assert.assertEquals(0, bucketSizeInfo[0].getFreeBuckets().size());
		Assert.assertEquals(8, bucketSizeInfo[0].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[1].getBucketList().size());
		Assert.assertEquals(0, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(8, bucketSizeInfo[1].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[2].getBucketList().size());
		Assert.assertEquals(0, bucketSizeInfo[2].getFreeBuckets().size());
		Assert.assertEquals(8, bucketSizeInfo[2].getCompletelyFreeBuckets().size());

		for (int i = 0; i < 3; i++) {
			int bucketCount = bucketSizeInfo[i].getBucketList().size();
			int itemSize = 1 << (i + 5);
			int itemCount = 128 / itemSize;
			for (Object object : bucketSizeInfo[i].getBucketList().keySet()) {
				Assert.assertEquals(itemSize, ((PowerTwoBucketAllocator.Bucket) object).getItemAllocationSize());
				Assert.assertEquals(itemCount, ((PowerTwoBucketAllocator.Bucket) object).freeCount());
			}

			Assert.assertEquals(0, bucketSizeInfo[i].statistics().usedCount());
			Assert.assertEquals(bucketCount * itemCount, bucketSizeInfo[i].statistics().freeCount());
			Assert.assertEquals(itemSize, bucketSizeInfo[i].statistics().itemSize());
		}

		//allocate 128, use distributed bucket
		int offset1 = bucketSizeInfo[2].allocateBlock();
		Assert.assertEquals(0, power.safeGetBucketNo(offset1));
		Assert.assertEquals(0, offset1);

		Assert.assertEquals(0, bucketSizeInfo[0].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[2].statistics().freeCount());
		Assert.assertEquals(1, bucketSizeInfo[2].statistics().usedCount());
		Assert.assertEquals(128, bucketSizeInfo[2].statistics().usedBytes());
		Assert.assertEquals(0, bucketSizeInfo[2].getFreeBuckets().size());
		Assert.assertEquals(7, bucketSizeInfo[2].getCompletelyFreeBuckets().size());
		Assert.assertEquals(1, bucketSizeInfo[2].getBucketList().size());

		//allocate 128 again, will ask info[0] to get bucket
		int offset2 = bucketSizeInfo[2].allocateBlock();
		Assert.assertEquals(1, power.safeGetBucketNo(offset2));
		Assert.assertEquals(128, offset2);

		Assert.assertEquals(0, bucketSizeInfo[0].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[2].statistics().freeCount());
		Assert.assertEquals(2, bucketSizeInfo[2].statistics().usedCount());
		Assert.assertEquals(256, bucketSizeInfo[2].statistics().usedBytes());

		Assert.assertEquals(0, bucketSizeInfo[2].getFreeBuckets().size());
		Assert.assertEquals(6, bucketSizeInfo[2].getCompletelyFreeBuckets().size());
		Assert.assertEquals(2, bucketSizeInfo[2].getBucketList().size());

		Assert.assertEquals(0, bucketSizeInfo[0].getFreeBuckets().size());
		Assert.assertEquals(6, bucketSizeInfo[0].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[0].getBucketList().size());

		Assert.assertEquals(0, bucketSizeInfo[1].getBucketList().size());
		Assert.assertEquals(0, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(6, bucketSizeInfo[1].getCompletelyFreeBuckets().size());

		//free 128, check bucket list
		power.free(offset2);

		Assert.assertEquals(0, bucketSizeInfo[0].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[2].statistics().freeCount());
		Assert.assertEquals(1, bucketSizeInfo[2].statistics().usedCount());
		Assert.assertEquals(128, bucketSizeInfo[2].statistics().usedBytes());

		Assert.assertEquals(0,
			((PowerTwoBucketAllocator.Bucket) bucketSizeInfo[2].getBucketList().get(0)).getBaseOffset());

		Assert.assertEquals(0, bucketSizeInfo[2].getFreeBuckets().size());
		Assert.assertEquals(7, bucketSizeInfo[2].getCompletelyFreeBuckets().size());
		Assert.assertEquals(1, bucketSizeInfo[2].getBucketList().size());

		Assert.assertEquals(0, bucketSizeInfo[0].getFreeBuckets().size());
		Assert.assertEquals(7, bucketSizeInfo[0].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[0].getBucketList().size());

		Assert.assertEquals(0, bucketSizeInfo[1].getBucketList().size());
		Assert.assertEquals(0, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(7, bucketSizeInfo[1].getCompletelyFreeBuckets().size());

		//allocate 64 check free and CompletelyFree
		int offset3 = bucketSizeInfo[1].allocateBlock();
		Assert.assertEquals(2, power.safeGetBucketNo(offset3));
		Assert.assertEquals(320, offset3);

		Assert.assertEquals(1, bucketSizeInfo[1].getBucketList().size());
		Assert.assertEquals(1, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(6, bucketSizeInfo[1].getCompletelyFreeBuckets().size());
		Assert.assertEquals(1, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(1, bucketSizeInfo[1].statistics().usedCount());
		Assert.assertEquals(64, bucketSizeInfo[1].statistics().usedBytes());

		//allocate 64 check free and CompletelyFree again
		int offset4 = bucketSizeInfo[1].allocateBlock();
		Assert.assertEquals(2, power.safeGetBucketNo(offset4));
		Assert.assertEquals(256, offset4);

		Assert.assertEquals(1, bucketSizeInfo[1].getBucketList().size());
		Assert.assertEquals(0, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(6, bucketSizeInfo[1].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(2, bucketSizeInfo[1].statistics().usedCount());
		Assert.assertEquals(128, bucketSizeInfo[1].statistics().usedBytes());

		//free 64, and check again
		power.free(offset3);
		Assert.assertEquals(1, bucketSizeInfo[1].getBucketList().size());
		Assert.assertEquals(1, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(6, bucketSizeInfo[1].getCompletelyFreeBuckets().size());
		Assert.assertEquals(1, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(1, bucketSizeInfo[1].statistics().usedCount());
		Assert.assertEquals(64, bucketSizeInfo[1].statistics().usedBytes());

		//free 64 and all are free, and check again
		power.free(offset4);
		Assert.assertEquals(0, bucketSizeInfo[1].getBucketList().size());
		Assert.assertEquals(0, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(7, bucketSizeInfo[1].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().usedCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().usedBytes());
	}

	@Test
	public void testNormal() {
		PowerTwoBucketAllocator power = new PowerTwoBucketAllocator(128, 64);

		PowerTwoBucketAllocator.Bucket[] allBuckets = power.getAllBuckets();
		PowerTwoBucketAllocator.BucketSizeInfo[] bucketSizeInfo = power.getBucketSizeInfos();

		Assert.assertEquals(2, allBuckets.length);
		Assert.assertEquals(2, bucketSizeInfo.length);

		Assert.assertEquals(0, bucketSizeInfo[0].getFreeBuckets().size());
		Assert.assertEquals(2, bucketSizeInfo[0].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[0].statistics().usedCount());
		Assert.assertEquals(0, bucketSizeInfo[0].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[0].statistics().usedBytes());

		Assert.assertEquals(0, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(2, bucketSizeInfo[1].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().usedCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().usedBytes());

		try {
			power.allocate(65);
			Assert.fail("should exception");
		} catch (Exception e) {
			Assert.assertEquals("PowerTwoBucketAllocator can't allocate size larger than bucket size", e.getMessage());
		}

		try {
			power.allocate(0);
			Assert.fail("should have thrown exception but not");
		} catch (IllegalArgumentException e) {
			Assert.assertThat(e.getMessage(), Matchers.is("size must be larger than 0"));
		}

		int offset1 = power.allocate(1);
		Assert.assertEquals(32, offset1);

		Assert.assertEquals(1, bucketSizeInfo[0].getFreeBuckets().size());
		Assert.assertEquals(1, bucketSizeInfo[0].getCompletelyFreeBuckets().size());
		Assert.assertEquals(1, bucketSizeInfo[0].statistics().usedCount());
		Assert.assertEquals(1, bucketSizeInfo[0].statistics().freeCount());
		Assert.assertEquals(32, bucketSizeInfo[0].statistics().usedBytes());

		Assert.assertEquals(0, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(1, bucketSizeInfo[1].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().usedCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().usedBytes());

		int offset2 = power.allocate(2);
		Assert.assertEquals(0, offset2);

		Assert.assertEquals(0, bucketSizeInfo[0].getFreeBuckets().size());
		Assert.assertEquals(1, bucketSizeInfo[0].getCompletelyFreeBuckets().size());
		Assert.assertEquals(2, bucketSizeInfo[0].statistics().usedCount());
		Assert.assertEquals(0, bucketSizeInfo[0].statistics().freeCount());
		Assert.assertEquals(64, bucketSizeInfo[0].statistics().usedBytes());

		Assert.assertEquals(0, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(1, bucketSizeInfo[1].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().usedCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().usedBytes());

		//allocate >32
		int offset3 = power.allocate(33);
		Assert.assertEquals(64, offset3);

		Assert.assertEquals(0, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[1].getCompletelyFreeBuckets().size());
		Assert.assertEquals(1, bucketSizeInfo[1].statistics().usedCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(64, bucketSizeInfo[1].statistics().usedBytes());

		Assert.assertEquals(NO_SPACE, power.allocate(32));

		//free error len
		try {
			power.free(0, 33);
			Assert.fail("should Exception");
		} catch (Exception e) {
			Assert.assertEquals("PowerTwoBucketAllocator free size not match", e.getMessage());
		}

		power.free(0);
		power.free(32, 31);
		power.free(64);

		Assert.assertEquals(0, bucketSizeInfo[0].getFreeBuckets().size());
		Assert.assertEquals(2, bucketSizeInfo[0].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[0].statistics().usedCount());
		Assert.assertEquals(0, bucketSizeInfo[0].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[0].statistics().usedBytes());

		Assert.assertEquals(0, bucketSizeInfo[1].getFreeBuckets().size());
		Assert.assertEquals(2, bucketSizeInfo[1].getCompletelyFreeBuckets().size());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().usedCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().freeCount());
		Assert.assertEquals(0, bucketSizeInfo[1].statistics().usedBytes());
	}

	@Test
	public void testConcurrentAllocateAndFree() throws InterruptedException {
		PowerTwoBucketAllocator power = new PowerTwoBucketAllocator(1024 * 1024, 128);

		PowerTwoBucketAllocator.Bucket[] allBuckets = power.getAllBuckets();
		PowerTwoBucketAllocator.BucketSizeInfo[] bucketSizeInfo = power.getBucketSizeInfos();

		Assert.assertEquals(8192, allBuckets.length);
		Assert.assertEquals(3, bucketSizeInfo.length);
		Assert.assertEquals(32, getBucketItemSizeFromIndex(bucketSizeInfo[0].sizeIndex()));
		Assert.assertEquals(64, getBucketItemSizeFromIndex(bucketSizeInfo[1].sizeIndex()));
		Assert.assertEquals(128, getBucketItemSizeFromIndex(bucketSizeInfo[2].sizeIndex()));

		int threadNum = 10;
		Thread[] threads = new Thread[threadNum];
		AtomicBoolean hasException = new AtomicBoolean(false);
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new Thread(() -> {
				int i1 = 0;
				while (i1++ < 100000) {
					try {
						int offset = power.allocate(45);
						if (offset != NO_SPACE) {
							power.free(offset);
						}
					} catch (Throwable e) {
						e.printStackTrace();
						hasException.set(true);
					}
				}
			});
		}

		for (Thread t : threads) {
			t.start();
		}
		for (Thread t : threads) {
			t.join();
		}
		Assert.assertFalse(hasException.get());
	}
}
