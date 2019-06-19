/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Contains test-cases to verify OzoneManagerLock.
 */
public class TestOzoneManagerLock {

  public void testS3BucketLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireS3BucketLock("s3Bucket");
    lock.releaseS3BucketLock("s3Bucket");
    Assert.assertTrue(true);
  }

  public void testSameS3BucketLockInSameThread() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireS3BucketLock("s3Bucket");
    lock.acquireS3BucketLock("s3Bucket");
    lock.releaseS3BucketLock("s3Bucket");
    lock.releaseS3BucketLock("s3Bucket");
    Assert.assertTrue(true);
  }

  @Test
  public void testS3BucketParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    AtomicBoolean gotLock = new AtomicBoolean(false);
    lock.acquireS3BucketLock("s3Bucket");

    Runnable run1 = (() -> {
      lock.acquireS3BucketLock("s3Bucket");
      gotLock.set(true);
      lock.releaseS3BucketLock("s3Bucket");
    });

    Thread thread1 = new Thread(run1);
    thread1.start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same user's, it will wait.
    Assert.assertFalse(gotLock.get());

    lock.releaseS3BucketLock("s3Bucket");

    // Since we have released the lock, the new thread should have the lock
    // now
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());

    Assert.assertTrue(true);
  }

  @Test
  public void testS3LockAfterAcquiringVolumeLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireVolumeLock("volOne");
    try {
      lock.acquireS3BucketLock("s3Bucket");
      Assert.fail();
    } catch (RuntimeException ex) {
      String msg =
          "cannot acquire S3 bucket lock while holding Ozone " +
              "Volume/Bucket/User lock(s).";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releaseVolumeLock("volOne");
    Assert.assertTrue(true);
  }

  @Test
  public void testS3LockAfterAcquiringBucketLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireBucketLock("volOne", "bucketOne");
    try {
      lock.acquireS3BucketLock("s3Bucket");
      Assert.fail();
    } catch (RuntimeException ex) {
      String msg =
          "cannot acquire S3 bucket lock while holding Ozone " +
              "Volume/Bucket/User lock(s).";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releaseBucketLock("volOne", "bucketOne");
    Assert.assertTrue(true);
  }

  @Test
  public void testS3LockAfterAcquiringUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireUserLock("userOne");
    try {
      lock.acquireS3BucketLock("s3Bucket");
      Assert.fail();
    } catch (RuntimeException ex) {
      String msg =
          "cannot acquire S3 bucket lock while holding Ozone " +
              "Volume/Bucket/User lock(s).";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releaseUserLock("userOne");
    Assert.assertTrue(true);
  }


  @Test(timeout = 1000)
  public void testDifferentUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireUserLock("userOne");
    try {
      lock.acquireUserLock("userTwo");
      Assert.fail("testDifferentUserLock");
    } catch (RuntimeException ex) {
      String message = "For acquiring lock on multiple users, use " +
          "acquireMultiLock method";
      Assert.assertTrue(ex.getMessage().contains(message));
    }
    lock.releaseUserLock("userOne");
    Assert.assertTrue(true);
  }

  @Test
  public void testMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireMultiUserLock("ozone", "hdfs");
    lock.releaseMultiUserLock("ozone", "hdfs");
    Assert.assertTrue(true);
  }

  @Test
  public void testMultiUserLockParallel() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    AtomicBoolean gotLock = new AtomicBoolean(false);
    lock.acquireMultiUserLock("hdfs", "ozone");

    Runnable run1 = (() -> {
      lock.acquireMultiUserLock("ozone", "hdfs");
      gotLock.set(true);
      lock.releaseMultiUserLock("ozone", "hdfs");
    });

    Thread thread1 = new Thread(run1);
    thread1.start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same user's, it will wait.
    Assert.assertFalse(gotLock.get());

    lock.releaseMultiUserLock("hdfs", "ozone");

    // Since we have released the lock, the new thread should have the lock
    // now
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());

    Assert.assertTrue(true);
  }

  @Test
  public void testSameUserLock() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireUserLock("userOne");
    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      lock.acquireUserLock("userOne");
      gotLock.set(true);
      lock.releaseUserLock("userOne");
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same user, it will wait.
    Assert.assertFalse(gotLock.get());
    lock.releaseUserLock("userOne");
    // Since we have released the lock, the new thread should have the lock
    // now
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());
  }

  @Test(timeout = 1000)
  public void testDifferentVolumeLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireVolumeLock("volOne");
    lock.acquireVolumeLock("volTwo");
    lock.releaseVolumeLock("volOne");
    lock.releaseVolumeLock("volTwo");
    Assert.assertTrue(true);
  }

  @Test
  public void testSameVolumeLock() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireVolumeLock("volOne");
    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      lock.acquireVolumeLock("volOne");
      gotLock.set(true);
      lock.releaseVolumeLock("volOne");
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same user, it will wait.
    Assert.assertFalse(gotLock.get());
    lock.releaseVolumeLock("volOne");
    // Since we have released the lock, the new thread should have the lock
    // now
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());
  }

  @Test(timeout = 1000)
  public void testDifferentBucketLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireBucketLock("volOne", "bucketOne");
    lock.acquireBucketLock("volOne", "bucketTwo");
    lock.releaseBucketLock("volOne", "bucketTwo");
    lock.releaseBucketLock("volOne", "bucketOne");
    Assert.assertTrue(true);
  }

  @Test
  public void testSameBucketLock() throws Exception {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireBucketLock("volOne", "bucketOne");
    AtomicBoolean gotLock = new AtomicBoolean(false);
    new Thread(() -> {
      lock.acquireBucketLock("volOne", "bucketOne");
      gotLock.set(true);
      lock.releaseBucketLock("volOne", "bucketOne");
    }).start();
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    // Since the new thread is trying to get lock on same user, it will wait.
    Assert.assertFalse(gotLock.get());
    lock.releaseBucketLock("volOne", "bucketOne");
    // Since we have released the lock, the new thread should have the lock
    // now
    // Let's give some time for the new thread to run
    Thread.sleep(100);
    Assert.assertTrue(gotLock.get());
  }

  @Test(timeout = 1000)
  public void testUserLockAfterAcquiringVolumeLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireVolumeLock("volOne");
    lock.acquireUserLock("userOne");
    lock.releaseUserLock("userOne");
    lock.releaseVolumeLock("volOne");
    Assert.assertTrue(true);
  }

  @Test(timeout = 1000)
  public void testUserLockAfterBucketLockAfterVolumeLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireVolumeLock("volOne");
    lock.acquireBucketLock("volOne", "bucketOne");
    lock.acquireUserLock("userOne");
    lock.releaseUserLock("userOne");
    lock.releaseBucketLock("volOne", "bucketOne");
    lock.releaseVolumeLock("volOne");
    Assert.assertTrue(true);
  }


  @Test
  public void testVolumeLockAfterUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireUserLock("userOne");
    try {
      lock.acquireVolumeLock("volOne");
      Assert.fail();
    } catch (RuntimeException ex) {
      String msg =
          "cannot acquire volume lock while holding " +
              "Bucket/User lock(s).";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releaseVolumeLock("volOne");
    Assert.assertTrue(true);
  }

  @Test
  public void testVolumeLockAfterBucketLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireBucketLock("volOne", "bucketOne");
    try {
      lock.acquireVolumeLock("volOne");
      Assert.fail();
    } catch (RuntimeException ex) {
      String msg =
          "cannot acquire volume lock while holding Bucket/User lock(s).";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releaseBucketLock("volOne", "bucketOne");
    Assert.assertTrue(true);
  }

  @Test(timeout = 1000)
  public void testBucketLockAfterAcquiringVolumeLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireVolumeLock("volOne");
    lock.acquireBucketLock("volOne", "bucketOne");
    lock.releaseBucketLock("volOne", "bucketOne");
    lock.releaseVolumeLock("volOne");
    Assert.assertTrue(true);
  }

  @Test(timeout = 1000)
  public void testBucketLockAfterAcquiringVolumeLockAfterAcquiringS3Lock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireS3BucketLock("s3Bucket");
    lock.acquireVolumeLock("volOne");
    lock.acquireBucketLock("volOne", "bucketOne");
    lock.releaseBucketLock("volOne", "bucketOne");
    lock.releaseVolumeLock("volOne");
    Assert.assertTrue(true);
  }

  @Test(timeout = 1000)
  public void testBucketLockAfterAcquiringUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireUserLock("Userone");
    try {
      lock.acquireBucketLock("volOne", "bucketOne");
      Assert.fail();
    } catch (RuntimeException ex) {
      String msg = "cannot acquire bucket lock while holding User lock.";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releaseUserLock("Userone");
    Assert.assertTrue(true);
  }

  @Test
  public void testVolumeLockAfterUserLockAfterBucketLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireBucketLock("volOne", "bucketOne");
    lock.acquireUserLock("UserOne");
    try {
      lock.acquireVolumeLock("volOne");
      Assert.fail();
    } catch (RuntimeException ex) {
      String msg =
          "cannot acquire volume lock while holding Bucket/User lock(s).";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releaseBucketLock("volOne", "bucketOne");
    Assert.assertTrue(true);
  }

  @Test
  public void testS3SecretLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireS3SecretLock("s3Secret");
    lock.releaseS3SecretLock("s3Secret");
    Assert.assertTrue(true);
  }

  @Test
  public void testS3SecretLockAcquireTwice() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquireS3SecretLock("s3Secret");
    try {
      lock.acquireS3SecretLock("s3Secret");
      Assert.fail("testS3SecretLockAcquireTwice failed");
    } catch (RuntimeException ex) {
      String msg =
          "cannot acquire S3 Secret lock while holding S3 awsAccessKey lock" +
              "(s).";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releaseS3SecretLock("s3Secret");
  }

  @Test
  public void testPrefixLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquirePrefixLock("prefix");
    lock.releasePrefixLock("prefix");
    Assert.assertTrue(true);
  }

  @Test
  public void testPrefixLockAcquireTwice() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    lock.acquirePrefixLock("prefix");
    try {
      lock.acquirePrefixLock("prefix");
      Assert.fail("testPrefixLockAcquireTwice failed");
    } catch (RuntimeException ex) {
      String msg =
          "cannot acquire prefix path lock while holding prefix path lock(s) " +
              "for path";
      Assert.assertTrue(ex.getMessage().contains(msg));
    }
    lock.releasePrefixLock("prefix");
  }


}