package org.apache.hadoop.ozone.om.lock;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Class tests OzoneManagerLock.
 */
public class TestOzoneManagerLock {

  @Test
  public void acquireS3BucketLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String s3Bucket = "s3Bucket";
    String resourceName =
        OzoneManagerLockUtil.generateS3BucketLockName(s3Bucket);
    lock.acquireLock(resourceName, OzoneManagerLock.Resource.S3_BUCKET);
    lock.releaseLock(resourceName, OzoneManagerLock.Resource.S3_BUCKET);
  }

  @Test
  public void reacquireS3BucketLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String s3Bucket = "s3Bucket";
    String resourceName =
        OzoneManagerLockUtil.generateS3BucketLockName(s3Bucket);
    lock.acquireLock(resourceName, OzoneManagerLock.Resource.S3_BUCKET);
    lock.acquireLock(resourceName, OzoneManagerLock.Resource.S3_BUCKET);
    lock.releaseLock(resourceName, OzoneManagerLock.Resource.S3_BUCKET);
    lock.releaseLock(resourceName, OzoneManagerLock.Resource.S3_BUCKET);
  }


  @Test
  public void acquireS3BucketLockWhenHoldingHigherOrderVolumeLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String s3Bucket = "s3Bucket";
    String s3BucketLockName =
        OzoneManagerLockUtil.generateS3BucketLockName(s3Bucket);
    String volumeLockName = OzoneManagerLockUtil.generateVolumeLockName(
        "volumeOzone");
    lock.acquireLock(volumeLockName, OzoneManagerLock.Resource.VOLUME);
    try {
      lock.acquireLock(s3BucketLockName,
          OzoneManagerLock.Resource.S3_BUCKET);
      fail("acquireS3BucketLockWhenHoldingHigherOrderLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire S3_BUCKET lock while holding [VOLUME] " +
          "lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }

    lock.releaseLock(volumeLockName, OzoneManagerLock.Resource.VOLUME);
  }

  @Test
  public void acquireS3BucketLockWhenHoldingHigherOrderBucketLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String s3Bucket = "s3Bucket";
    String s3BucketLockName =
        OzoneManagerLockUtil.generateS3BucketLockName(s3Bucket);
    String bucketLockName = OzoneManagerLockUtil.generateVolumeLockName(
        "bucketOzone");
    lock.acquireLock(bucketLockName, OzoneManagerLock.Resource.BUCKET);
    try {
      lock.acquireLock(s3BucketLockName,
          OzoneManagerLock.Resource.S3_BUCKET);
      fail("acquireS3BucketLockWhenHoldingHigherOrderLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire S3_BUCKET lock while holding [BUCKET] " +
          "lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }

    lock.releaseLock(bucketLockName, OzoneManagerLock.Resource.BUCKET);
  }


  @Test
  public void acquireS3BucketLockWhenHoldingHigherOrderUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String s3Bucket = "s3Bucket";
    String s3BucketLockName =
        OzoneManagerLockUtil.generateS3BucketLockName(s3Bucket);
    String userLockName = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    lock.acquireLock(userLockName, OzoneManagerLock.Resource.USER);
    try {
      lock.acquireLock(s3BucketLockName,
          OzoneManagerLock.Resource.S3_BUCKET);
      fail("acquireS3BucketLockWhenHoldingHigherOrderLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire S3_BUCKET lock while holding [USER] " +
          "lock(s).";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }

    lock.releaseLock(userLockName, OzoneManagerLock.Resource.USER);
  }

  @Test
  public void acquireUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String userLockName = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    lock.acquireLock(userLockName, OzoneManagerLock.Resource.USER);
    lock.releaseLock(userLockName, OzoneManagerLock.Resource.USER);
  }

  @Test
  public void reAcquireUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String userLockName = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    lock.acquireLock(userLockName, OzoneManagerLock.Resource.USER);
    try {
      lock.acquireLock(userLockName, OzoneManagerLock.Resource.USER);
      fail("reAcquireUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }

    lock.releaseLock(userLockName, OzoneManagerLock.Resource.USER);
  }

  @Test
  public void acquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    String newUserLock = OzoneManagerLockUtil.generateUserLockName("user2");
    lock.acquireMultiUserLock(oldUserLock, newUserLock);
    lock.releaseMultiUserLock(oldUserLock, newUserLock);
  }

  @Test
  public void reAcquireMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    String newUserLock = OzoneManagerLockUtil.generateUserLockName("user2");
    lock.acquireMultiUserLock(oldUserLock, newUserLock);
    try {
      lock.acquireMultiUserLock(oldUserLock, newUserLock);
      fail("reAcquireMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock(oldUserLock, newUserLock);
  }

  @Test
  public void acquireMultiUserLockAfterUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    String newUserLock = OzoneManagerLockUtil.generateUserLockName("user2");
    String userLock = OzoneManagerLockUtil.generateUserLockName("user3");
    lock.acquireLock(userLock, OzoneManagerLock.Resource.USER);
    try {
      lock.acquireMultiUserLock(oldUserLock, newUserLock);
      fail("acquireMultiUserLockAfterUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER lock ";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseLock(userLock, OzoneManagerLock.Resource.USER);
  }

  @Test
  public void acquireUserLockAfterMultiUserLock() {
    OzoneManagerLock lock = new OzoneManagerLock(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    String newUserLock = OzoneManagerLockUtil.generateUserLockName("user2");
    String userLock = OzoneManagerLockUtil.generateUserLockName("user3");
    lock.acquireMultiUserLock(oldUserLock, newUserLock);
    try {
      lock.acquireLock(userLock, OzoneManagerLock.Resource.USER);
      fail("acquireUserLockAfterMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock(oldUserLock, newUserLock);
  }

  @Test
  public void releaseLockWithOutAcquiringLock() {
    String userLock = OzoneManagerLockUtil.generateUserLockName("user3");
    OzoneManagerLock lock =
        new OzoneManagerLock(new OzoneConfiguration());
    try {
      lock.releaseLock(userLock, OzoneManagerLock.Resource.USER);
      fail("releaseLockWithOutAcquiringLock failed");
    } catch (IllegalMonitorStateException ex) {
      String message = "Releasing lock on resource $user3 without acquiring " +
          "lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
  }
}
