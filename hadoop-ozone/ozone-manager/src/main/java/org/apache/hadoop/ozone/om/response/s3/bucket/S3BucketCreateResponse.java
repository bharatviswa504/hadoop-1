package org.apache.hadoop.ozone.om.response.s3.bucket;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

import java.io.IOException;

/**
 * Response for S3Bucket create request.
 */
public class S3BucketCreateResponse extends OMClientResponse {

  private OMVolumeCreateResponse omVolumeCreateResponse;
  private OMBucketCreateResponse omBucketCreateResponse;
  private String s3Bucket;
  private String s3Mapping;

  public S3BucketCreateResponse(OMVolumeCreateResponse omVolumeCreateResponse,
      OMBucketCreateResponse omBucketCreateResponse, String s3BucketName,
      String s3Mapping, OMResponse omResponse) {
    super(omResponse);
    this.omVolumeCreateResponse = omVolumeCreateResponse;
    this.omBucketCreateResponse = omBucketCreateResponse;
    this.s3Bucket = s3BucketName;
    this.s3Mapping = s3Mapping;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      if (omVolumeCreateResponse != null) {
        omVolumeCreateResponse.addToDBBatch(omMetadataManager, batchOperation);
      }

      if (omBucketCreateResponse != null) {
        omBucketCreateResponse.addToDBBatch(omMetadataManager, batchOperation);
      }

      omMetadataManager.getS3Table().putWithBatch(batchOperation, s3Bucket,
          s3Mapping);
    }
  }
}
