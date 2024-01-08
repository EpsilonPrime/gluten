/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.glutenproject.execution;

import io.substrait.proto.Plan;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.LoggerFactory;

public class AdbcClient {
  private static final String substraitVersion = "0.40.0";

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AdbcClient.class);

  static void call(Plan plan) {
    try {
      // MEGAHACK -- Make this an ADBC client instead.
      BufferAllocator allocator = new RootAllocator();
      Location clientLocation = Location.forGrpcInsecure("localhost", 8888);
      FlightSqlClient client =
          new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());
      FlightSqlClient.SubstraitPlan substraitPlan =
          new FlightSqlClient.SubstraitPlan(plan.toByteArray(), substraitVersion);
      FlightInfo info = client.executeSubstrait(substraitPlan);
      LOG.error("info is " + info.toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
