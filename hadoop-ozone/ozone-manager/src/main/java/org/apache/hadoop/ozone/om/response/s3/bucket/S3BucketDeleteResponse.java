package org.apache.hadoop.ozone.om.response.s3.bucket;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

import java.io.IOException;

/**
 * Response for S3Bucket Delete request.
 */
public class S3BucketDeleteResponse extends OMClientResponse {

  private String s3BucketName;
  private String volumeName;
  public S3BucketDeleteResponse(String s3BucketName,
      String volumeName, OMResponse omResponse) {
    super(omResponse);
    this.s3BucketName = s3BucketName;
    this.volumeName = volumeName;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      omMetadataManager.getBucketTable().deleteWithBatch(batchOperation,
          omMetadataManager.getBucketKey(volumeName, s3BucketName));
      omMetadataManager.getS3Table().deleteWithBatch(batchOperation,
          s3BucketName);
    }
  }
}
