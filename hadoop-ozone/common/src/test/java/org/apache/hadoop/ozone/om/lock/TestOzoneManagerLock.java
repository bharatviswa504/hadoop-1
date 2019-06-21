package org.apache.hadoop.ozone.om.lock;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.fail;

public class TestOzoneManagerLock {

  @Test
  public void acquireS3BucketLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String s3Bucket = "s3Bucket";
    String resourceName =
        OzoneManagerLockUtil.generateS3BucketLockName(s3Bucket);
    lock.acquireLock(resourceName, OzoneManagerLockAnu.Resource.S3_BUCKET);
    lock.releaseLock(resourceName, OzoneManagerLockAnu.Resource.S3_BUCKET);
  }

  @Test
  public void reacquireS3BucketLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String s3Bucket = "s3Bucket";
    String resourceName =
        OzoneManagerLockUtil.generateS3BucketLockName(s3Bucket);
    lock.acquireLock(resourceName, OzoneManagerLockAnu.Resource.S3_BUCKET);
    lock.acquireLock(resourceName, OzoneManagerLockAnu.Resource.S3_BUCKET);
    lock.releaseLock(resourceName, OzoneManagerLockAnu.Resource.S3_BUCKET);
    lock.releaseLock(resourceName, OzoneManagerLockAnu.Resource.S3_BUCKET);
  }


  @Test
  public void acquireS3BucketLockWhenHoldingHigherOrderVolumeLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String s3Bucket = "s3Bucket";
    String s3BucketLockName =
        OzoneManagerLockUtil.generateS3BucketLockName(s3Bucket);
    String volumeLockName = OzoneManagerLockUtil.generateVolumeLockName(
        "volumeOzone");
    lock.acquireLock(volumeLockName, OzoneManagerLockAnu.Resource.VOLUME);
    try {
      lock.acquireLock(s3BucketLockName, OzoneManagerLockAnu.Resource.S3_BUCKET);
      fail("acquireS3BucketLockWhenHoldingHigherOrderLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire S3_BUCKET lock while holding VOLUME " +
          "lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }

    lock.releaseLock(volumeLockName, OzoneManagerLockAnu.Resource.VOLUME);
  }

  @Test
  public void acquireS3BucketLockWhenHoldingHigherOrderBucketLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String s3Bucket = "s3Bucket";
    String s3BucketLockName =
        OzoneManagerLockUtil.generateS3BucketLockName(s3Bucket);
    String bucketLockName = OzoneManagerLockUtil.generateVolumeLockName(
        "bucketOzone");
    lock.acquireLock(bucketLockName, OzoneManagerLockAnu.Resource.BUCKET);
    try {
      lock.acquireLock(s3BucketLockName, OzoneManagerLockAnu.Resource.S3_BUCKET);
      fail("acquireS3BucketLockWhenHoldingHigherOrderLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire S3_BUCKET lock while holding BUCKET " +
          "lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }

    lock.releaseLock(bucketLockName, OzoneManagerLockAnu.Resource.BUCKET);
  }


  @Test
  public void acquireS3BucketLockWhenHoldingHigherOrderUserLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String s3Bucket = "s3Bucket";
    String s3BucketLockName =
        OzoneManagerLockUtil.generateS3BucketLockName(s3Bucket);
    String userLockName = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    lock.acquireLock(userLockName, OzoneManagerLockAnu.Resource.USER);
    try {
      lock.acquireLock(s3BucketLockName, OzoneManagerLockAnu.Resource.S3_BUCKET);
      fail("acquireS3BucketLockWhenHoldingHigherOrderLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire S3_BUCKET lock while holding USER " +
          "lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }

    lock.releaseLock(userLockName, OzoneManagerLockAnu.Resource.USER);
  }

  @Test
  public void acquireUserLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String userLockName = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    lock.acquireLock(userLockName, OzoneManagerLockAnu.Resource.USER);
    lock.releaseLock(userLockName, OzoneManagerLockAnu.Resource.USER);
  }

  @Test
  public void reAcquireUserLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String userLockName = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    lock.acquireLock(userLockName, OzoneManagerLockAnu.Resource.USER);
    try {
      lock.acquireLock(userLockName, OzoneManagerLockAnu.Resource.USER);
      fail("reAcquireUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER lock while holding USER " +
          "lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }

    lock.releaseLock(userLockName, OzoneManagerLockAnu.Resource.USER);
  }

  @Test
  public void acquireMultiUserLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    String newUserLock = OzoneManagerLockUtil.generateUserLockName("user2");
    lock.acquireMultiUserLock(oldUserLock, newUserLock);
    lock.releaseMultiUserLock(oldUserLock, newUserLock);
  }

  @Test
  public void reAcquireMultiUserLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    String newUserLock = OzoneManagerLockUtil.generateUserLockName("user2");
    lock.acquireMultiUserLock(oldUserLock, newUserLock);
    try {
      lock.acquireMultiUserLock(oldUserLock, newUserLock);
      fail("reAcquireMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire MultiUser lock while holding MultiUser " +
          "lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock(oldUserLock, newUserLock);
  }

  @Test
  public void acquireMultiUserLockAfterUserLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    String newUserLock = OzoneManagerLockUtil.generateUserLockName("user2");
    String userLock = OzoneManagerLockUtil.generateUserLockName("user3");
    lock.acquireLock(userLock, OzoneManagerLockAnu.Resource.USER);
    try {
      lock.acquireMultiUserLock(oldUserLock, newUserLock);
      fail("acquireMultiUserLockAfterUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire MultiUser lock while holding " +
          "User/higher order level lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseLock(userLock, OzoneManagerLockAnu.Resource.USER);
  }

  @Test
  public void acquireUserLockAfterMultiUserLock() {
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    String oldUserLock = OzoneManagerLockUtil.generateUserLockName(
        "user1");
    String newUserLock = OzoneManagerLockUtil.generateUserLockName("user2");
    String userLock = OzoneManagerLockUtil.generateUserLockName("user3");
    lock.acquireMultiUserLock(oldUserLock, newUserLock);
    try {
      lock.acquireLock(userLock, OzoneManagerLockAnu.Resource.USER);
      fail("acquireUserLockAfterMultiUserLock failed");
    } catch (RuntimeException ex) {
      String message = "cannot acquire USER lock while holding " +
          "MultiUser lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
    lock.releaseMultiUserLock(oldUserLock, newUserLock);
  }

  @Test
  public void releaseLockWithOutAcquiringLock() {
    String userLock = OzoneManagerLockUtil.generateUserLockName("user3");
    OzoneManagerLockAnu lock = new OzoneManagerLockAnu(new OzoneConfiguration());
    try {
      lock.releaseLock(userLock, OzoneManagerLockAnu.Resource.USER);
      fail("releaseLockWithOutAcquiringLock failed");
    } catch (IllegalMonitorStateException ex) {
      String message = "Releasing lock on resource $user3 without acquiring " +
          "lock";
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
  }
}
