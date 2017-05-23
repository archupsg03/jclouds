/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jclouds.openstack.swift.v1.blobstore.functions;

import org.jclouds.io.ContentMetadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class ContentMetaDataToMap {

   public ImmutableMap<String, String> getContentMetadataForManifest(ContentMetadata contentMetadata) {
      Builder<String, String> mapBuilder = ImmutableMap.builder();
      if (contentMetadata.getContentType() != null) {
         mapBuilder.put("content-type", contentMetadata.getContentType());
      }
      /**
       * Do not set content-length. Set automatically to manifest json string length by BindToJsonPayload
       */
      if (contentMetadata.getContentDisposition() != null) {
         mapBuilder.put("content-disposition", contentMetadata.getContentDisposition());
      }
      if (contentMetadata.getContentEncoding() != null) {
         mapBuilder.put("content-encoding", contentMetadata.getContentEncoding());
      }
      if (contentMetadata.getContentLanguage() != null) {
         mapBuilder.put("content-language", contentMetadata.getContentLanguage());
      }
      return mapBuilder.build();
   }
}
