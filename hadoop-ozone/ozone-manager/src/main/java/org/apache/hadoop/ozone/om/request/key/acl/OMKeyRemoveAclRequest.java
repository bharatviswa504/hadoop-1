/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.key.acl;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.key.acl.OMKeyAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.util.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveAclResponse;

/**
 * Handle add Acl request for bucket.
 */
public class OMKeyRemoveAclRequest extends OMKeyAclRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyAddAclRequest.class);

  private static BiFunction<List<OzoneAclInfo>, OmKeyInfo > keyRemoveAclOp;
  private String path;
  private List<OzoneAclInfo> ozoneAcls;

  static {
    keyRemoveAclOp = (ozoneAcls, omKeyInfo) -> {
      return omKeyInfo.removeAcl(ozoneAcls.get(0));
    };
  }

  public OMKeyRemoveAclRequest(OMRequest omRequest) {
    super(omRequest, keyRemoveAclOp);
    OzoneManagerProtocolProtos.RemoveAclRequest removeAclRequest =
        getOmRequest().getRemoveAclRequest();
    path = removeAclRequest.getObj().getPath();
    ozoneAcls = Lists.newArrayList(removeAclRequest.getAcl());
  }

  @Override
  List<OzoneAclInfo> getAcls() {
    return ozoneAcls;
  }

  @Override
  String getPath() {
    return path;
  }

  @Override
  OMResponse.Builder onInit() {
    return OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.RemoveAcl).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

  }

  @Override
  OMClientResponse onSuccess(OMResponse.Builder omResponse,
      OmKeyInfo omKeyInfo, boolean operationResult) {
    omResponse.setSuccess(operationResult);
    omResponse.setRemoveAclResponse(RemoveAclResponse.newBuilder()
        .setResponse(operationResult));
    return new OMKeyAclResponse(omKeyInfo,
        omResponse.build());
  }

  @Override
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException exception) {
    return new OMKeyAclResponse(null,
        createErrorOMResponse(omResponse, exception));
  }

  @Override
  void onComplete(boolean operationResult, IOException exception,
      OMMetrics omMetrics) {
    if (operationResult) {
      LOG.debug("Remove acl: {} to path: {} success!", getAcls(), getPath());
    } else {
      omMetrics.incNumBucketUpdateFails();
      if (exception == null) {
        LOG.error("Remove acl {} to path {} failed, because acl already exist",
            getAcls(), getPath());
      } else {
        LOG.error("Remove acl {} to path {} failed!", getAcls(), getPath(),
            exception);
      }
    }
  }

}

