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

package org.apache.hadoop.ozone.om.request.key.acl.prefix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.PrefixManagerImpl.OMPrefixAclOpResult;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.request.util.ObjectParser;
import org.apache.hadoop.ozone.om.response.key.acl.prefix.OMPrefixAclResponse;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.util.QuadFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetAclResponse;

/**
 * Handle add Acl request for bucket.
 */
public class OMPrefixSetAclRequest extends OMPrefixAclRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMPrefixAddAclRequest.class);

  private static QuadFunction<OzoneObj, List<OzoneAcl>, OmPrefixInfo,
      PrefixManagerImpl, OMPrefixAclOpResult, IOException> prefixSetAclOp;
  private OzoneObj ozoneObj;
  private List<OzoneAcl> ozoneAcls;


  static {
    prefixSetAclOp = (ozoneObj, ozoneAcls, omPrefixInfo, prefixManager) -> {
      return prefixManager.setAcl(ozoneObj, ozoneAcls, omPrefixInfo);
    };
  }

  public OMPrefixSetAclRequest(OMRequest omRequest) {
    super(omRequest, prefixSetAclOp);
    OzoneManagerProtocolProtos.SetAclRequest setAclRequest =
        getOmRequest().getSetAclRequest();
    // TODO: conversion of OzoneObj to protobuf can be avoided when we
    //  single code path for HA and Non-HA
    ozoneObj = OzoneObjInfo.fromProtobuf(setAclRequest.getObj());
    ozoneAcls = new ArrayList<>();
    setAclRequest.getAclList().forEach(aclInfo ->
        ozoneAcls.add(OzoneAcl.fromProtobuf(aclInfo)));
  }

  @Override
  List<OzoneAcl> getAcls() {
    return ozoneAcls;
  }

  @Override
  OzoneObj getOzoneObj() {
    return ozoneObj;
  }

  @Override
  OMResponse.Builder onInit() {
    return OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.SetAcl).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

  }

  @Override
  OMClientResponse onSuccess(OMResponse.Builder omResponse,
      OmPrefixInfo omPrefixInfo, boolean operationResult) {
    omResponse.setSuccess(operationResult);
    omResponse.setSetAclResponse(SetAclResponse.newBuilder()
        .setResponse(operationResult));
    return new OMPrefixAclResponse(omPrefixInfo,
        omResponse.build());
  }

  @Override
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException exception) {
    return new OMPrefixAclResponse(null,
        createErrorOMResponse(omResponse, exception));
  }

  @Override
  void onComplete(boolean operationResult, IOException exception,
      OMMetrics omMetrics) {
    if (operationResult) {
      LOG.debug("Set acl: {} to path: {} success!", getAcls(),
          getOzoneObj().getPath());
    } else {
      omMetrics.incNumBucketUpdateFails();
      if (exception == null) {
        LOG.error("Set acl {} to path {} failed", getAcls(),
            getOzoneObj().getPath());
      } else {
        LOG.error("Set acl {} to path {} failed!", getAcls(),
            getOzoneObj().getPath(), exception);
      }
    }
  }

}

