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
package org.jclouds.openstack.swift.v1.options;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.jclouds.blobstore.options.PutOptions;

public class OpenStackSwiftPutOptions extends PutOptions {

   private boolean dlo = false; // SLO is enabled

   public OpenStackSwiftPutOptions() {
   }

   public OpenStackSwiftPutOptions(boolean upload_type) {
      this.dlo = upload_type;
   }

   protected OpenStackSwiftPutOptions(boolean multipart, boolean useCustomExecutor,
         ListeningExecutorService customExecutor, boolean upload_type) {
      super(multipart, useCustomExecutor, customExecutor);
      this.dlo = upload_type;
   }

   public static class ImmutablePutOptions extends OpenStackSwiftPutOptions {
      private final OpenStackSwiftPutOptions delegate;

      public ImmutablePutOptions(OpenStackSwiftPutOptions delegate) {
         this.delegate = delegate;
      }

      @Override
      public boolean isDLO() {
         return delegate.isDLO();
      }

      @Override
      public OpenStackSwiftPutOptions DLO() {
         return DLO(true);
      }

      @Override
      public OpenStackSwiftPutOptions clone() {
         return delegate.clone();
      }

      @Override
      public String toString() {
         return delegate.toString();
      }
   }

   public boolean isDLO() {
      return dlo;
   }

   public OpenStackSwiftPutOptions DLO() {
      return DLO(true);
   }

   public OpenStackSwiftPutOptions DLO(boolean val) {
      this.dlo = val;
      return this;
   }

   public OpenStackSwiftPutOptions DLO(ListeningExecutorService customExecutor) {
      this.dlo = true;
      return this;
   }

   public static class Builder {

      public static OpenStackSwiftPutOptions fromPutOptions(OpenStackSwiftPutOptions putOptions) {
         return DLO(putOptions.dlo);
      }

      public static OpenStackSwiftPutOptions DLO() {
         return DLO(true);
      }

      public static OpenStackSwiftPutOptions DLO(boolean val) {
         OpenStackSwiftPutOptions options = new OpenStackSwiftPutOptions();
         return options.DLO(val);
      }
   }

   public OpenStackSwiftPutOptions clone() {
      return new OpenStackSwiftPutOptions(multipart, useCustomExecutor, customExecutor, dlo);
   }

   @Override
   public String toString() {
      return "[upload_type=" + dlo + "]";
   }
}
