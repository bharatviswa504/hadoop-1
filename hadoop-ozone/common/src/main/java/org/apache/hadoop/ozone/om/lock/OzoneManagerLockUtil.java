package org.apache.hadoop.ozone.om.lock;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_SECRET;
import static org.apache.hadoop.ozone.OzoneConsts.OM_USER_PREFIX;
import static org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy.LOG;

/**
 * Utility class contains helper functions required for OM lock.
 */
public final class OzoneManagerLockUtil {


  private OzoneManagerLockUtil() {
  }


  public static String generateResourceLockName(
      OzoneManagerLock.Resource resource, String... resources) {

    if (resources.length == 1) {
      if (resource == OzoneManagerLock.Resource.S3_BUCKET) {
        return OM_S3_PREFIX + resources[0];
      } else if (resource == OzoneManagerLock.Resource.VOLUME) {
        return OM_KEY_PREFIX + resources[0];
      } else if (resource == OzoneManagerLock.Resource.USER) {
        return OM_USER_PREFIX + resources[0];
      } else if (resource == OzoneManagerLock.Resource.S3_SECRET) {
        return OM_S3_SECRET + resources[0];
      } else if (resource == OzoneManagerLock.Resource.PREFIX) {
        return OM_S3_PREFIX + resources[0];
      } else {
        throw new IllegalArgumentException("Unidentified resource type is" +
            " passed when Resource type is bucket");
      }
    } else if (resources.length == 2) {
      if (resource == OzoneManagerLock.Resource.BUCKET) {
        return OM_KEY_PREFIX + resources[0] + OM_KEY_PREFIX + resources[1];
      } else {
        throw new IllegalArgumentException("VolumeName/BucketName should be" +
            " passed when Resource type is bucket");
      }
    } else {
      throw new IllegalArgumentException("This should be called with only " +
          "maximum 2 resource names");
    }
  }

}
