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

public class OzoneManagerLockAnu {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerLock.class);

  private final LockManager<String> manager;
  private final ThreadLocal<Short> lockSet = ThreadLocal.withInitial(
      () -> new Short((short)0));


  /**
   * Creates new OzoneManagerLock instance.
   * @param conf Configuration object
   */
  public OzoneManagerLockAnu(Configuration conf) {
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
    if (!resource.canLock(lockSet.get())) {
      throw new RuntimeException(getErrorMessage(resource));
    } else {
      manager.lock(resourceName);
      lockSet.set(resource.setLock(lockSet.get()));
    }
  }

  private String getErrorMessage(Resource resource) {
    for (Resource value : Resource.values()) {
      if (lockSet.get() >= resource.setMask) {
          return "Thread '" + Thread.currentThread().getName() + "' cannot " +
              "acquire " + resource.name + " lock while holding " + value.name +
              " lock";
        }
      }
    return null;
  }

  /**
   * Acquire lock on multiple users.
   * @param oldUserResource
   * @param newUserResource
   */
  public void acquireMultiUserLock(String oldUserResource,
      String newUserResource) {
    Resource resource = Resource.USER;
    if (!resource.canLock(lockSet.get())) {
      throw new RuntimeException(getErrorMessage(resource));
    } else {
      int compare = newUserResource.compareTo(oldUserResource);
      if (compare < 0) {
        manager.lock(newUserResource);
        manager.lock(oldUserResource);
      } else if (compare > 0) {
        manager.lock(oldUserResource);
        manager.lock(newUserResource);
      } else {
        // both users are equal.
        manager.lock( oldUserResource);
      }
      lockSet.set(resource.setLock(lockSet.get()));
    }
  }

  /**
   * Acquire lock on multiple users.
   * @param oldUserResource
   * @param newUserResource
   */
  public void releaseMultiUserLock(String oldUserResource,
      String newUserResource) {
    Resource resource = Resource.USER;
    int compare = newUserResource.compareTo(oldUserResource);
    if (compare < 0) {
      manager.unlock(newUserResource);
      manager.unlock(oldUserResource);
    } else if (compare > 0) {
      manager.unlock(oldUserResource);
      manager.unlock(newUserResource);
    } else {
      // both users are equal.
      manager.lock(oldUserResource);
    }
    lockSet.set(resource.clearLock(lockSet.get()));
  }


  public void releaseLock(String resourceName, Resource resource) {

    // TODO: Not checking release of higher order level lock happened while
    // releasing lower order level lock, as for that we need counter for
    // locks, as some locks support acquiring lock again.
    manager.unlock(resourceName);// Unset the bit.
    lockSet.set(resource.clearLock(lockSet.get()));

  }

  public enum Resource {
    // For S3 Bucket need to allow only for S3, that should be means only 1.
    S3_BUCKET((byte)0,  "S3_BUCKET"), // = 1

    // For volume need to allow both s3 bucket and volume. 01 + 10 = 11 (3)
    VOLUME((byte)1,  "VOLUME"), // = 2

    // For bucket we need to allow both s3 bucket, volume and bucket. Which
    // is equal to 100 + 010 + 001 = 111 = 4 + 2 + 1 = 7
    BUCKET((byte)2,  "BUCKET"), // = 4

    // For user we need to allow s3 bucket, volume, bucket and user lock.
    // Which is 8  4 + 2 + 1 = 15
    USER((byte)3,  "USER"), // 15

    S3_SECRET((byte)4, "S3_SECRET"), // 31
    PREFIX((byte)5, "PREFIX"); //63

    // level of the resource
    byte pos;

    // This will tell the value till which we can allow locking.
    short mask;

    // This value will help during setLock, and also will tell whether we can
    // re-acquire lock or not.
    short setMask;

    // Name of the resource.
    String name;

    Resource(byte pos, String name) {
      this.pos = pos;
      for (int x = 0; x < pos + 1; x++) {
        this.mask  += (short) Math.pow(2, pos);
      }
      this.setMask = (short) Math.pow(2, pos);
      this.name = name;
    }

    boolean canLock(short lockSet) {

      // For USER, S3_SECRET and  PREFIX we shall not allow re-acquire locks at
      // from single thread.
      if ((USER.setMask & lockSet) == USER.setMask ||
          ((S3_SECRET.setMask & lockSet) == S3_SECRET.setMask) ||
          (PREFIX.setMask & lockSet) == PREFIX.setMask) {
        return false;
      }


      // Our mask is the summation of bits of all previous possible locks. In
      // other words it is the largest possible value for that bit position.

      // For example for Volume lock, bit position is 1, and mask is 3. Which
      // is the largest value that can be represented with 2 bits is 3.
      // Therefore if lockSet is larger than mask we have to return false i.e
      // some other higher order lock has been acquired.

      return lockSet <= mask;
    }

    short setLock(short lockSetVal) {
      System.out.println("acquire" + (short) (lockSetVal | setMask));
      return (short) (lockSetVal | setMask);
    }

    short clearLock(short lockSetVal) {
      System.out.println("release" + (short) (lockSetVal & ~setMask));
      return (short) (lockSetVal & ~setMask);
    }

    short getMask() {
      return mask;
    }

  }

}

