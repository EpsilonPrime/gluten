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
package io.glutenproject.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.ProjectExec

class VeloxLiteralSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-literal-velox"
  override protected val fileFormat: String = "parquet"
  override protected val backend: String = "velox"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
  }

  def validateOffloadResult(sql: String): Unit = {
    runQueryAndCompare(sql) {
      df =>
        val plan = df.queryExecution.executedPlan
        assert(plan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined, sql)
        assert(plan.find(_.isInstanceOf[ProjectExec]).isEmpty, sql)
    }
  }

  test("Struct Literal") {
    validateOffloadResult("SELECT struct('Spark', 5)")
    validateOffloadResult("SELECT struct(7, struct(5, 'test'))")
    validateOffloadResult("SELECT struct(-0.1, array(5, 6))")
    validateOffloadResult("SELECT struct(7, map('red', 1, 'green', 2))")
    validateOffloadResult("SELECT struct(array(5, 6), map('red', 1, 'green', 2))")
    validateOffloadResult("SELECT struct(1.0, struct(array(5, 6), map('red', 1)))")
    validateOffloadResult("SELECT struct(5, 1S, 1Y, -1Y, true, false)")
    validateOffloadResult("SELECT struct(1D, 1F)")
    validateOffloadResult("SELECT struct(5.321E2BD, 0.1, 5.321E22BD)")
    validateOffloadResult("SELECT struct(TIMESTAMP'2020-12-31')")
    validateOffloadResult("SELECT struct(X'1234')")
    validateOffloadResult("SELECT struct(DATE'2020-12-31')")
  }
}
