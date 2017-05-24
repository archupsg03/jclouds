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
package org.jclouds.openstack.swift.v1.features;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jclouds.io.Payloads.newByteSourcePayload;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jclouds.io.Payloads;
import org.jclouds.openstack.swift.v1.domain.ObjectList;
import org.jclouds.openstack.swift.v1.domain.Segment;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.internal.BaseSwiftApiLiveTest;
import org.jclouds.openstack.swift.v1.options.ListContainerOptions;
import org.jclouds.utils.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;

@Test(groups = "live", testName = "DynamicLargeObjectApiLiveTest", singleThreaded = true)
public class DynamicLargeObjectApiLiveTest extends BaseSwiftApiLiveTest {

   private String defaultName = getClass().getSimpleName();
   private String defaultContainerName = getClass().getSimpleName() + "Container";
   private ByteSource megOf1s;
   private ByteSource megOf2s;
   private String objectName = "myObject";

   @Test
   public void testReplaceManifest() throws Exception {
      for (String regionId : regions) {
         assertReplaceManifest(regionId, defaultContainerName, defaultName);
         uploadLargeFile(regionId);
      }
   }

   @SuppressWarnings("deprecation")
   @Test
   public void uploadLargeFile(String regionId) throws IOException, InterruptedException {
      List<Segment> segmentList = new ArrayList<Segment>();
      int partNumber = 1;
      int total_size = 0;
      // configure the blobstore to use multipart uploading of the file
      for (int i = partNumber; i < 3; partNumber++) {
         String objName = String.format("%s/%s/%s", objectName, "dlo", partNumber);
         String data = String.format("%s%s", "data", partNumber);
         String etag = getApi().getDynamicLargeObjectApi(regionId, defaultContainerName).uploadPart(
               defaultContainerName, objName, Payloads.newPayload(data), ImmutableMap.of("myfoo", "Bar"),
               ImmutableMap.of("myfoo", "Bar"));
         Segment s = new Segment(objName, etag, data.length());
         assertNotNull(etag);
         segmentList.add(s);
         total_size += data.length();
      }
      String etagOfEtags = getApi().getDynamicLargeObjectApi(regionId, defaultContainerName).replaceManifest(
            objectName, segmentList, ImmutableMap.of("MyFoo", "Bar"), ImmutableMap.of("MyFoo", "Bar"));

      SwiftObject bigObject = getApi().getObjectApi(regionId, defaultContainerName).get(objectName);
      assertThat(bigObject.getETag().equals(etagOfEtags));
      assertThat(bigObject.getPayload().getContentMetadata().getContentLength().equals(Long.valueOf(total_size)));
      assertThat(bigObject.getMetadata().equals(ImmutableMap.of("myfoo", "Bar")));
      assertThat(getApi().getContainerApi(regionId).get(defaultContainerName).getObjectCount().equals(Long.valueOf(3)));
   }

   protected void assertReplaceManifest(String regionId, String containerName, String name) {
      ObjectApi objectApi = getApi().getObjectApi(regionId, containerName);

      String etag1s = objectApi.put(name + "/1", newByteSourcePayload(megOf1s));
      awaitConsistency();
      assertMegabyteAndETagMatches(regionId, containerName, name + "/1", etag1s);

      String etag2s = objectApi.put(name + "/2", newByteSourcePayload(megOf2s));
      awaitConsistency();
      assertMegabyteAndETagMatches(regionId, containerName, name + "/2", etag2s);

      List<Segment> segments = ImmutableList.<Segment> builder()
            .add(Segment.builder().path(format("%s/%s/1", containerName, name)).etag(etag1s).sizeBytes(1024 * 1024)
                  .build())
            .add(Segment.builder().path(format("%s/%s/2", containerName, name)).etag(etag2s).sizeBytes(1024 * 1024)
                  .build())
            .build();

      awaitConsistency();
      String etagOfEtags = getApi().getDynamicLargeObjectApi(regionId, containerName).replaceManifest(name, segments,
            ImmutableMap.of("myfoo", "Bar"), ImmutableMap.of("header1", "value1"));

      assertNotNull(etagOfEtags);

      awaitConsistency();

      SwiftObject bigObject = getApi().getObjectApi(regionId, containerName).get(name);
      assertThat(bigObject.getETag().equals(etagOfEtags));
      assertThat(bigObject.getPayload().getContentMetadata().getContentLength().equals(Long.valueOf(2 * 1024 * 1024)));
      assertThat(bigObject.getMetadata().equals(ImmutableMap.of("myfoo", "Bar")));

      // segments are visible
      assertThat(getApi().getContainerApi(regionId).get(containerName).getObjectCount().equals(Long.valueOf(3)));
   }

   protected void assertMegabyteAndETagMatches(String regionId, String containerName, String name, String etag1s) {
      SwiftObject object1s = getApi().getObjectApi(regionId, containerName).get(name);
      assertThat(object1s.getETag().equals(etag1s));
      assertThat(object1s.getPayload().getContentMetadata().getContentLength().equals(Long.valueOf(1024 * 1024)));
   }

   protected void deleteAllObjectsInContainerDLO(String regionId, final String containerName) {
       ObjectList objects = getApi().getObjectApi(regionId, containerName).list(new ListContainerOptions());
      if (objects == null) {
         return;
      }
      List<String> pathsToDelete = Lists.transform(objects, new Function<SwiftObject, String>() {
         public String apply(SwiftObject input) {
            return containerName + "/" + input.getName();
         }
      });
      if (!pathsToDelete.isEmpty()) {
         for (String name : pathsToDelete)
            getApi().getObjectApi(regionId, containerName).delete(name);
      }
   }

   @Override
   @BeforeClass(groups = "live")
   public void setup() {
      super.setup();
      for (String regionId : regions) {
         boolean created = getApi().getContainerApi(regionId).create(defaultContainerName);
         if (!created) {
            deleteAllObjectsInContainer(regionId, defaultContainerName);
         }
      }

     /* megOf1s = new byte[1024 * 1024];
      megOf2s = new byte[1024 * 1024];

      Arrays.fill(megOf1s, (byte) 1);
      Arrays.fill(megOf2s, (byte) 2);*/
      
      megOf1s = TestUtils.randomByteSource().slice(0, 1048576);
      megOf2s = TestUtils.randomByteSource().slice(0, 1048576);
   }

   @AfterClass(groups = "live")
   public void tearDown() {
      for (String regionId : regions) {
         deleteAllObjectsInContainerDLO(regionId, defaultContainerName);
         getApi().getContainerApi(regionId).deleteIfEmpty(defaultContainerName);
      }
   }
}
