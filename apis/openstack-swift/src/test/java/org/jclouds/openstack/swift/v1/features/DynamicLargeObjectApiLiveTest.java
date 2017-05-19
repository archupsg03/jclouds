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
import static org.jclouds.io.Payloads.newByteSourcePayload;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jclouds.openstack.swift.v1.domain.ObjectList;
import org.jclouds.openstack.swift.v1.domain.Segment;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.internal.BaseSwiftApiLiveTest;
import org.jclouds.openstack.swift.v1.options.ListContainerOptions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.Uninterruptibles;

@Test(groups = "live", testName = "DynamicLargeObjectApiLiveTest", singleThreaded = true)
public class DynamicLargeObjectApiLiveTest extends BaseSwiftApiLiveTest {

   private String defaultName = getClass().getSimpleName();
   private String defaultContainerName = getClass().getSimpleName() + "Container";
   private byte[] megOf1s;
   private byte[] megOf2s;

   @Test
   public void testReplaceManifest() throws Exception {
      for (String regionId : regions) {
         assertReplaceManifest(regionId, defaultContainerName, defaultName);
      }
   }

   protected void assertReplaceManifest(String regionId, String containerName, String name) {
      ObjectApi objectApi = getApi().getObjectApi(regionId, containerName);

      String etag1s = objectApi.put(name + "/1", newByteSourcePayload(ByteSource.wrap(megOf1s)));
      awaitConsistency();
      assertMegabyteAndETagMatches(regionId, containerName, name + "/1", etag1s);

      String etag2s = objectApi.put(name + "/2", newByteSourcePayload(ByteSource.wrap(megOf2s)));
      awaitConsistency();
      assertMegabyteAndETagMatches(regionId, containerName, name + "/2", etag2s);

      List<Segment> segments = ImmutableList.<Segment> builder()
            .add(Segment.builder().path(format("%s/%s/1", containerName, name)).etag(etag1s).sizeBytes(1024 * 1024)
                  .build())
            .add(Segment.builder().path(format("%s/%s/2", containerName, name)).etag(etag2s).sizeBytes(1024 * 1024)
                  .build())
            .build();

      awaitConsistency();
      String etagOfEtags = getApi().getDynamicLargeObjectApi(regionId, containerName).replaceManifest(containerName,
            name, segments, ImmutableMap.of("myfoo", "Bar"), ImmutableMap.of("header1", "value1"));

      assertNotNull(etagOfEtags);

      awaitConsistency();

      SwiftObject bigObject = getApi().getObjectApi(regionId, containerName).get(name);
      assertEquals(bigObject.getETag(), etagOfEtags);
      assertEquals(bigObject.getPayload().getContentMetadata().getContentLength(), Long.valueOf(2 * 1024 * 1024));
      assertEquals(bigObject.getMetadata(), ImmutableMap.of("myfoo", "Bar"));

      // segments are visible
      assertEquals(getApi().getContainerApi(regionId).get(containerName).getObjectCount(), Long.valueOf(3));
   }

   protected void assertMegabyteAndETagMatches(String regionId, String containerName, String name, String etag1s) {
      SwiftObject object1s = getApi().getObjectApi(regionId, containerName).get(name);
      assertEquals(object1s.getETag(), etag1s);
      assertEquals(object1s.getPayload().getContentMetadata().getContentLength(), Long.valueOf(1024 * 1024));
   }

   protected void deleteAllObjectsInContainerDLO(String regionId, final String containerName) {
      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

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

      megOf1s = new byte[1024 * 1024];
      megOf2s = new byte[1024 * 1024];

      Arrays.fill(megOf1s, (byte) 1);
      Arrays.fill(megOf2s, (byte) 2);
   }

   @AfterClass(groups = "live")
   public void tearDown() {
      for (String regionId : regions) {
         deleteAllObjectsInContainerDLO(regionId, defaultContainerName);
         getApi().getContainerApi(regionId).deleteIfEmpty(defaultContainerName);
      }
   }
}
