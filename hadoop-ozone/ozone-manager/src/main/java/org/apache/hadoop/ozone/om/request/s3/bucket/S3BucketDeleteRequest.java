package org.apache.hadoop.ozone.om.request.s3.bucket;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.bucket.S3BucketCreateResponse;
import org.apache.hadoop.ozone.om.response.s3.bucket.S3BucketDeleteResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3DeleteBucketRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class S3BucketDeleteRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3BucketDeleteRequest.class);

  public S3BucketDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    S3DeleteBucketRequest s3DeleteBucketRequest =
        getOmRequest().getDeleteS3BucketRequest();
    Preconditions.checkNotNull(s3DeleteBucketRequest);

    // TODO: Do we need to enforce the bucket rules in this code path?
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html

    // For now only checked the length.
    int bucketLength = s3DeleteBucketRequest.getS3BucketName().length();
    if (bucketLength < 3 || bucketLength >= 64) {
      throw new OMException("S3BucketName must be at least 3 and not more " +
          "than 63 characters long",
          OMException.ResultCodes.S3_BUCKET_INVALID_LENGTH);
    }

    return getOmRequest().toBuilder().setUserInfo(getUserInfo()).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex) {
    S3DeleteBucketRequest s3DeleteBucketRequest =
        getOmRequest().getDeleteS3BucketRequest();
    Preconditions.checkNotNull(s3DeleteBucketRequest);

    String s3BucketName = s3DeleteBucketRequest.getS3BucketName();

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setCmdType(
            OzoneManagerProtocolProtos.Type.DeleteS3Bucket).setStatus(
            OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumS3BucketDeletes();
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.S3, IAccessAuthorizer.ACLType.DELETE, null,
            s3BucketName, null);
      }
    } catch (IOException ex) {
      LOG.error("S3Bucket Deletion failed for S3Bucket:{}", s3BucketName, ex);
      omMetrics.incNumS3BucketDeleteFails();
      auditLog(ozoneManager.getAuditLogger(),
          buildAuditMessage(OMAction.DELETE_S3_BUCKET,
              buildAuditMap(s3BucketName), ex, getOmRequest().getUserInfo()));
      return new S3BucketDeleteResponse(null, null,
          createErrorOMResponse(omResponse, ex));
    }

    IOException exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    omMetadataManager.getLock().acquireS3Lock(s3BucketName);

    boolean needToReleaseLock = false;
    String volumeName = null;
    try {
      String s3Mapping = omMetadataManager.getS3Table().get(s3BucketName);

      if (s3Mapping == null) {
        throw new OMException("S3Bucket " + s3BucketName + " not found",
            OMException.ResultCodes.S3_BUCKET_NOT_FOUND);
      } else {
        volumeName = getOzoneVolumeName(s3Mapping);

        omMetadataManager.getLock().acquireBucketLock(volumeName, s3BucketName);
        needToReleaseLock = true;

        String bucketKey = omMetadataManager.getBucketKey(volumeName,
            s3BucketName);

        // Update bucket table cache and s3 table cache.
        omMetadataManager.getBucketTable().addCacheEntry(
            new CacheKey<>(bucketKey),
            new CacheValue<>(Optional.absent(), transactionLogIndex));
        omMetadataManager.getS3Table().addCacheEntry(
            new CacheKey<>(s3BucketName),
            new CacheValue<>(Optional.absent(), transactionLogIndex));
      }
    } catch (IOException ex) {
      exception = ex;
    } finally {
      if (needToReleaseLock) {
        omMetadataManager.getLock().releaseBucketLock(volumeName, s3BucketName);
      }
      omMetadataManager.getLock().releaseS3Lock(s3BucketName);
    }

    // Performing audit logging outside of the lock.
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.DELETE_S3_BUCKET,
            buildAuditMap(s3BucketName), exception,
            getOmRequest().getUserInfo()));

    if (exception == null) {
      // Decrement s3 bucket and ozone bucket count. As S3 bucket is mapped to
      // ozonevolume/ozone bucket.
      LOG.debug("S3Bucket {} successfully deleted", s3BucketName);
      omMetrics.decNumS3Buckets();
      omMetrics.decNumBuckets();
      return new S3BucketDeleteResponse(s3BucketName, volumeName,
          omResponse.build());
    } else {
      LOG.error("S3Bucket {} successfully deleted", s3BucketName, exception);
      omMetrics.incNumS3BucketDeleteFails();
      return new S3BucketDeleteResponse(null, null,
          createErrorOMResponse(omResponse, exception));
    }
  }

  /**
   * Extract volumeName from s3Mapping.
   * @param s3Mapping
   * @return volumeName
   * @throws IOException
   */
  private String getOzoneVolumeName(String s3Mapping) throws IOException {
    return s3Mapping.split("/")[0];
  }

  private Map<String, String> buildAuditMap(String s3BucketName) {
    Map<String, String> auditMap = new HashMap<>();
    auditMap.put(s3BucketName, OzoneConsts.S3_BUCKET);
    return auditMap;
  }

}
