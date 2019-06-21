package org.apache.hadoop.ozone.om.lock;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_SECRET;
import static org.apache.hadoop.ozone.OzoneConsts.OM_USER_PREFIX;

/**
 * Utility class contains helper functions required for OM lock.
 */
public final class OzoneManagerLockUtil {

  /**
   * Given a s3BucketName return corresponding resource name to be
   * used in lock manager to acquire/release lock.
   * @param s3BucketName
   */
  public static String generateS3BucketLockName(String s3BucketName) {
    return OM_S3_PREFIX + s3BucketName;
  }

  /**
   * Given a volumeName return corresponding resource name to be
   * used in lock manager to acquire/release lock.
   * @param volumeName
   */
  public static String generateVolumeLockName(String volumeName) {
    return OM_KEY_PREFIX + volumeName;
  }

  /**
   * Given volumeName and bucketName return corresponding resource name to be
   * used in lock manager to acquire/release lock.
   * @param volumeName
   * @param bucketName
   */
  public static String generateBucketLockName(String volumeName, String bucketName) {
    return OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName;
  }

  /**
   * Given a userName return corresponding resource name to be
   * used in lock manager to acquire/release lock.
   * @param userName
   */
  public static String generateUserLockName(String userName) {
    return OM_USER_PREFIX + userName;
  }

  /**
   * Given a s3Secret return corresponding resource name to be
   * used in lock manager to acquire/release lock.
   * @param s3Secret
   */
  public static  String generateS3SecretLockName(String s3Secret) {
    return OM_S3_SECRET + s3Secret;
  }

  /**
   * Given a prefix path return corresponding resource name to be
   * used in lock manager to acquire/release lock.
   * @param prefix
   */
  public static  String generatePrefixLockName(String prefix) {
    return OM_S3_PREFIX + prefix;
  }

}
