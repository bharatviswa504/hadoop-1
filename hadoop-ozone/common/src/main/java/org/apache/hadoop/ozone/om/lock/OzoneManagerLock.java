package org.apache.hadoop.ozone.om.lock;


import java.util.BitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.lock.LockManager;


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
 * {@literal ->} acquire volume lock (will work)<br>
 *   {@literal +->} acquire bucket lock (will work)<br>
 *     {@literal +-->} acquire s3 bucket lock (will throw Exception)<br>
 * </p>
 * <br>
 */

public class OzoneManagerLock {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerLock.class);

  private final LockManager<String> manager;
  private final ThreadLocal<BitSet> lockSet = ThreadLocal.withInitial(
      () -> new BitSet(Resource.values().length));
  private final ThreadLocal<Boolean> acquireMultiUserLock =
      ThreadLocal.withInitial(() -> false);


  /**
   * Creates new OzoneManagerLock instance.
   * @param conf Configuration object
   */
  public OzoneManagerLock(Configuration conf) {
    manager = new LockManager<>(conf);
  }

  /**
   * Acquire lock on resource.
   *
   * For S3_Bucket, VOLUME, BUCKET type resource, same thread acquiring lock
   * again is allowed.
   *
   * For USER, PREFIX, S3_SECRET type resource, same thread acquiring lock
   * again is not allowed.
   *
   * Special Not for UserLock: Single thread can acquire single user lock/
   * multi user lock. But not both at the same time.
   * @param resourceName - Resource name on which user want to acquire lock.
   * @param resource - Type of the resource.
   */
  public void acquireLock(String resourceName, Resource resource) {

    // When resource type is user, check if we have acquired multi user lock.
    if (resource == Resource.USER && acquireMultiUserLock.get()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() + "' cannot acquire "
              + "USER lock while holding MultiUser lock");
    } else {
      int higherLockSetPos;
      if (resource == Resource.S3_BUCKET || resource == Resource.VOLUME ||
          resource == Resource.BUCKET) {
        // For these locks we allow acquiring again from same thread. There are
        // some use cases for file operations which need to reacquire lock. So,
        // we check if we have acquired any higher order lock
        higherLockSetPos = hasAcquiredLock(resource.ordinal() + 1);
      } else {
        // For these locks, we don't allow re-acquire lock. So, we should
        // check if we have acquired the same resource lock and also higher order
        // lock.
        higherLockSetPos = hasAcquiredLock(resource.ordinal());
      }

      if (higherLockSetPos != -1) {
        throw new RuntimeException(
            "Thread '" + Thread.currentThread().getName() + "' cannot acquire "
                + resource.name() + " lock while holding " +
                Resource.values()[higherLockSetPos] + " lock");
      }
      manager.lock(resourceName);
      // Set the bit set
      lockSet.get().set(resource.ordinal(), true);
    }
  }

  /**
   * Acquire lock on multiple users.
   * @param oldUserResource
   * @param newUserResource
   */
  public void acquireMultiUserLock(String oldUserResource,
      String newUserResource) {

    // TODO: For now not checking already acquired lock on user names. Revisit
    //  this in future if we need to.
    int higherLockSetPos = hasAcquiredLock(Resource.USER.ordinal());
    if (acquireMultiUserLock.get()){
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() + "' cannot acquire "
              + "MultiUser lock while holding MultiUser lock");
    } else if (higherLockSetPos >= Resource.USER.ordinal()) {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() + "' cannot acquire "
              + "MultiUser lock while holding User/higher order level lock");
    } else {
      int compare = newUserResource.compareTo(oldUserResource);
      if (compare < 0) {
        manager.lock(newUserResource);
        manager.lock(oldUserResource);
      } else if (compare > 0) {
        manager.lock( oldUserResource);
        manager.lock( newUserResource);
      } else {
        // both users are equal.
        manager.lock( oldUserResource);
      }
      lockSet.get().set(Resource.USER.ordinal(), true);
      acquireMultiUserLock.set(true);
    }
  }

  /**
   * Acquire lock on multiple users.
   * @param oldUserResource
   * @param newUserResource
   */
  public void releaseMultiUserLock(String oldUserResource,
      String newUserResource) {
    if (acquireMultiUserLock.get()) {
      int compare = newUserResource.compareTo(oldUserResource);
      if (compare < 0) {
        manager.unlock(newUserResource);
        manager.unlock(oldUserResource);
      } else if (compare > 0) {
        manager.unlock( oldUserResource);
        manager.unlock( newUserResource);
      } else {
        // both users are equal.
        manager.unlock( oldUserResource);
      }
      lockSet.get().set(Resource.USER.ordinal(), false);
      acquireMultiUserLock.set(true);
    } else {
      throw new RuntimeException(
          "Thread '" + Thread.currentThread().getName() + "' cannot release "
              + " MultiUserLock, without holding MultiUserLock");
    }

  }


  public void releaseLock(String resourceName, Resource resource) {

    // TODO: Not checking release of higher order level lock happened while
    // releasing lower order level lock, as for that we need counter for
    // locks, as some locks support acquiring lock again.
    manager.unlock(resourceName);// Unset the bit.
    lockSet.get().set(resource.ordinal(), false);

  }

  private int hasAcquiredLock(int position) {
    return lockSet.get().nextSetBit(position);
  }

  public enum Resource {
    S3_BUCKET,
    VOLUME,
    BUCKET,
    USER,
    S3_SECRET,
    PREFIX
  }

}
