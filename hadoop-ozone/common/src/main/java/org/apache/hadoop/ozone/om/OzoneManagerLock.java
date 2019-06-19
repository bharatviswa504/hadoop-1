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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.lock.LockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_USER_PREFIX;

/**
 * Provides different locks to handle concurrency in OzoneMaster.
 * We also maintain lock hierarchy, based on the weight.
 *
 * <table>
 *   <caption></caption>
 *   <tr>
 *     <td><b> WEIGHT </b></td> <td><b> LOCK </b></td>
 *   </tr>
 *   <tr>
 *     <td> 0 </td> <td> S3 Bucket Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 1 </td> <td> Volume Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 2 </td> <td> Bucket Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 3 </td> <td> User Lock </td>
 *   </tr>
 *   <tr>
 *     <td> 4 </td> <td> S3 Secret Lock</td>
 *   </tr>
 *   <tr>
 *     <td> 5 </td> <td> Prefix Lock </td>
 *   </tr>
 * </table>
 *
 * One cannot obtain a lower weight lock while holding a lock with higher
 * weight. The other way around is possible. <br>
 * <br>
 * <p>
 * For example:
 * <br>
 * {@literal ->} acquireVolumeLock (will work)<br>
 *   {@literal +->} acquireBucketLock (will work)<br>
 *     {@literal +-->} acquireS3BucketLock (will throw Exception)<br>
 * </p>
 * <br>
 * To acquire a S3 lock you should not hold any Volume/Bucket lock. Similarly
 * to acquire a Volume lock you should not hold any Bucket/User/S3
 * Secret/Prefix lock.
 */
public final class OzoneManagerLock {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerLock.class);

  private static final String S3_BUCKET_LOCK = "s3BucketLock";
  private static final String VOLUME_LOCK = "volumeLock";
  private static final String BUCKET_LOCK = "bucketLock";
  private static final String USER_LOCK = "userLock";
  private static final String S3_SECRET_LOCK = "s3SecretetLock";
  private static final String PREFIX_LOCK = "prefixLock";


  private final LockManager<String> manager;

  // To maintain locks held by current thread.
  private final ThreadLocal<Map<String, AtomicInteger>> myLocks =
      ThreadLocal.withInitial(
          () -> ImmutableMap.<String, AtomicInteger>builder()
              .put(S3_BUCKET_LOCK, new AtomicInteger(0))
              .put(VOLUME_LOCK, new AtomicInteger(0))
              .put(BUCKET_LOCK, new AtomicInteger(0))
              .put(USER_LOCK, new AtomicInteger(0))
              .put(S3_SECRET_LOCK, new AtomicInteger(0))
              .put(PREFIX_LOCK, new AtomicInteger(0))
              .build()
      );

  /**
   * Creates new OzoneManagerLock instance.
   * @param conf Configuration object
   */
  public OzoneManagerLock(Configuration conf) {
    manager = new LockManager<>(conf);
  }

  /**
   * Acquires S3 Bucket lock on the given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the lock has
   * been acquired.
   *
   * @param s3BucketName S3Bucket Name on which the lock has to be acquired
   */
  public void acquireS3BucketLock(String s3BucketName) {
    // Calling thread should not hold any volume/bucket/user lock.

    // Not added checks for prefix/s3 secret lock, as they will never be
    // taken with s3Bucket Lock. In this way, we can avoid 2 checks every
    // time we acquire s3Bucket lock.

    // Or do we need to add this for future safe?

    if (hasAnyVolumeLock() || hasAnyBucketLock() || hasAnyUserLock()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() +
              "' cannot acquire S3 bucket lock while holding Ozone " +
              "Volume/Bucket/User lock(s).");
    }
    manager.lock(OM_S3_PREFIX + s3BucketName);
    myLocks.get().get(S3_BUCKET_LOCK).incrementAndGet();
  }

  /**
   * Releases the S3Bucket lock on given resource.
   */
  public void releaseS3BucketLock(String s3BucketName) {
    manager.unlock(OM_S3_PREFIX + s3BucketName);
    myLocks.get().get(S3_BUCKET_LOCK).decrementAndGet();
  }

  /**
   * Acquires volume lock on the given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   *
   * @param volume Volume on which the lock has to be acquired
   */
  public void acquireVolumeLock(String volume) {
    // Calling thread should not hold any bucket/user lock.
    // You can take an Volume while holding S3 bucket lock, since
    // semantically an S3 bucket maps to the ozone volume.
    if (hasAnyBucketLock() || hasAnyUserLock()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() + "' cannot acquire " +
              "volume lock while holding Bucket/User lock(s).");
    }
    manager.lock(OM_KEY_PREFIX + volume);
    myLocks.get().get(VOLUME_LOCK).incrementAndGet();
  }

  /**
   * Releases the volume lock on given resource.
   */
  public void releaseVolumeLock(String volume) {
    manager.unlock(OM_KEY_PREFIX + volume);
    myLocks.get().get(VOLUME_LOCK).decrementAndGet();
  }

  /**
   * Acquires bucket lock on the given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   *
   * @param bucket Bucket on which the lock has to be acquired
   */
  public void acquireBucketLock(String volume, String bucket) {
    if (hasAnyUserLock()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() +
              "' cannot acquire bucket lock while holding User lock.");
    }
    manager.lock(OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket);
    myLocks.get().get(BUCKET_LOCK).incrementAndGet();
  }

  /**
   * Releases the bucket lock on given resource.
   */
  public void releaseBucketLock(String volume, String bucket) {
    manager.unlock(OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket);
    myLocks.get().get(BUCKET_LOCK).decrementAndGet();
  }

  /**
   * Acquires user lock on the given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   *
   * @param user User on which the lock has to be acquired
   */
  public void acquireUserLock(String user) {
    // In order to not maintain username's on which we have acquired lock,
    // just checking have we acquired userLock before. If user want's to
    // acquire user lock on multiple user's they should use
    // acquireMultiUserLock. This is just a protection logic, to let not users
    // use this if acquiring lock on multiple users. As currently, we have only
    // use case we have for this is during setOwner operation in VolumeManager.
    if (hasAnyUserLock()) {
      LOG.error("Already have userLock");
      throw new RuntimeException("For acquiring lock on multiple users, use " +
          "acquireMultiLock method");
    }
    manager.lock(OM_USER_PREFIX + user);
    myLocks.get().get(USER_LOCK).incrementAndGet();
  }

  /**
   * Releases the user lock on given resource.
   */
  public void releaseUserLock(String user) {
    manager.unlock(OM_USER_PREFIX + user);
    myLocks.get().get(USER_LOCK).decrementAndGet();
  }

  /**
   * Acquire user lock on 2 users. In this case, we compare 2 strings
   * lexicographically, and acquire the locks according to the sorted order of
   * the user names. In this way, when acquiring locks on multiple user's, we
   * can avoid dead locks. This method should be called when single thread is
   * acquiring lock on 2 users at a time.
   *
   * Example:
   * ozone, hdfs -> lock acquire order will be hdfs, ozone
   * hdfs, ozone -> lock acquire order will be hdfs, ozone
   *
   * @param newUser
   * @param oldUser
   */
  public void acquireMultiUserLock(String newUser, String oldUser) {
    Preconditions.checkNotNull(newUser);
    Preconditions.checkNotNull(oldUser);
    int compare = newUser.compareTo(oldUser);

    if (compare < 0) {
      manager.lock(OM_USER_PREFIX + newUser);
      manager.lock(OM_USER_PREFIX + oldUser);
      myLocks.get().get(USER_LOCK).incrementAndGet();
      myLocks.get().get(USER_LOCK).incrementAndGet();
    } else if (compare > 0) {
      manager.lock(OM_USER_PREFIX + oldUser);
      manager.lock(OM_USER_PREFIX + newUser);
      myLocks.get().get(USER_LOCK).incrementAndGet();
      myLocks.get().get(USER_LOCK).incrementAndGet();
    } else {
      // both users are equal.
      manager.lock(OM_USER_PREFIX + oldUser);
      myLocks.get().get(USER_LOCK).incrementAndGet();
    }
  }

  /**
   * Release user lock on the provided resources.
   * @param newUser
   * @param oldUser
   */
  public void releaseMultiUserLock(String newUser, String oldUser) {
    Preconditions.checkNotNull(newUser);
    Preconditions.checkNotNull(oldUser);
    int compare = newUser.compareTo(oldUser);

    if (compare < 0) {
      manager.unlock(OM_USER_PREFIX + newUser);
      manager.unlock(OM_USER_PREFIX + oldUser);
      myLocks.get().get(USER_LOCK).decrementAndGet();
      myLocks.get().get(USER_LOCK).decrementAndGet();
    } else if (compare > 0) {
      manager.unlock(OM_USER_PREFIX + oldUser);
      manager.unlock(OM_USER_PREFIX + newUser);
      myLocks.get().get(USER_LOCK).decrementAndGet();
      myLocks.get().get(USER_LOCK).decrementAndGet();
    } else {
      // both users are equal.
      manager.lock(OM_USER_PREFIX + oldUser);
      myLocks.get().get(USER_LOCK).decrementAndGet();
    }
  }

  /**
   * Acquires s3 secret lock on the given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   *
   * @param awsAccessId on which the lock has to be acquired
   */
  public void acquireS3SecretLock(String awsAccessId) {
    if (hasAnyS3SecretLock()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() +
              "' cannot acquire S3 Secret lock while holding S3 " +
              "awsAccessKey lock(s).");
    }
    manager.lock(awsAccessId);
    myLocks.get().get(S3_SECRET_LOCK).incrementAndGet();
  }

  /**
   * Releases the s3 secret lock on given resource.
   */
  public void releaseS3SecretLock(String awsAccessId) {
    manager.unlock(awsAccessId);
    myLocks.get().get(S3_SECRET_LOCK).decrementAndGet();
  }

  /**
   * Acquires prefix lock on the given resource.
   *
   * <p>If the lock is not available then the current thread becomes
   * disabled for thread scheduling purposes and lies dormant until the
   * lock has been acquired.
   *
   * @param prefixPath on which the lock has to be acquired
   */
  public void acquirePrefixLock(String prefixPath) {
    if (hasAnyPrefixLock()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() +
              "' cannot acquire prefix path lock while holding prefix " +
              "path lock(s) for path: " + prefixPath + ".");
    }
    manager.lock(prefixPath);
    myLocks.get().get(PREFIX_LOCK).incrementAndGet();
  }

  /**
   * Releases the prefix lock on given resource.
   */
  public void releasePrefixLock(String prefixPath) {
    manager.unlock(prefixPath);
    myLocks.get().get(PREFIX_LOCK).decrementAndGet();
  }

  /**
   * Returns true if the current thread holds any volume lock.
   * @return true if current thread holds volume lock, else false
   */
  private boolean hasAnyVolumeLock() {
    return myLocks.get().get(VOLUME_LOCK).get() != 0;
  }

  /**
   * Returns true if the current thread holds any bucket lock.
   * @return true if current thread holds bucket lock, else false
   */
  private boolean hasAnyBucketLock() {
    return myLocks.get().get(BUCKET_LOCK).get() != 0;
  }

  /**
   * Returns true if the current thread holds any s3 bucket lock.
   * @return true if current thread holds s3 bucket lock, else false
   */
  private boolean hasAnyS3BucketLock() {
    return myLocks.get().get(S3_BUCKET_LOCK).get() != 0;
  }

  /**
   * Returns true if the current thread holds any user lock.
   * @return true if current thread holds user lock, else false
   */
  private boolean hasAnyUserLock() {
    return myLocks.get().get(USER_LOCK).get() != 0;
  }

  /**
   * Returns true if the current thread holds any s3 secret lock.
   * @return true if current thread holds s3 secret lock, else false
   */
  private boolean hasAnyS3SecretLock() {
    return myLocks.get().get(S3_SECRET_LOCK).get() != 0;
  }

  /**
   * Returns true if the current thread holds any prefix lock.
   * @return true if current thread holds prefix lock, else false
   */
  private boolean hasAnyPrefixLock() {
    return myLocks.get().get(PREFIX_LOCK).get() != 0;
  }


}
