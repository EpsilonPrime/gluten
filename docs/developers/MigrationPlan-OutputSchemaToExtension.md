---
layout: page
title: Migration Plan - Move output_schema to AdvancedExtension
nav_order: 11
parent: Developer Overview
---

# Migration Plan: Move RelRoot.output_schema to AdvancedExtension

**Status:** Ready for Implementation
**Priority:** Medium
**Estimated Effort:** 2-4 hours
**Issue Reference:** Part of Substrait unfork effort

---

## Background

### Current State

Gluten has a custom modification to `RelRoot` in `algebra.proto`:

```proto
message RelRoot {
  Rel input = 1;
  repeated string names = 2;
  Type.Struct output_schema = 3;  // CUSTOM FIELD - not in official Substrait
}
```

This field was added to fix **issue-1874**: ensuring output columns have the correct nullability when there's a mismatch between the logical plan and actual column types.

### Official Substrait v0.77.0

The official version only has:

```proto
message RelRoot {
  Rel input = 1;
  repeated string names = 2;
  // No output_schema field
}
```

### Current Usage in Gluten

**Set Location:** `gluten-core/src/main/java/io/glutenproject/substrait/plan/PlanNode.java:79`
```java
if (outputSchema != null) {
  relRootBuilder.setOutputSchema(outputSchema.toProtobuf().getStruct());
}
```

**Read Location:** `cpp-ch/local-engine/Parser/SerializedPlanParser.cpp:486-505`
```cpp
const auto & output_schema = root_rel.root().output_schema();
if (output_schema.types_size())
{
    // Check for nullability mismatches and apply corrections
    // Fixes issue-1874
}
```

**Purpose:**
- Preserve correct nullability information across the plan
- Fix type mismatches between Spark's logical plan and physical execution
- Ensure intermediate aggregate data types are handled correctly

---

## Migration Strategy

We will move `output_schema` from a direct field in `RelRoot` to an `AdvancedExtension` field.

### Why AdvancedExtension?

1. **Maintains functionality** - No loss of features
2. **Aligns with official Substrait** - RelRoot structure matches v0.77.0
3. **Future-proof** - Can potentially upstream this extension to Substrait
4. **Low risk** - Extension mechanism is well-supported

---

## Implementation Plan

### Phase 1: Create Extension Definition (30 minutes)

#### Step 1.1: Create extension proto file

Create new file: `gluten-core/src/main/resources/io/glutenproject/proto/gluten_substrait_extensions.proto`

```proto
// SPDX-License-Identifier: Apache-2.0
syntax = "proto3";

package io.glutenproject.extension;

import "substrait/type.proto";

option java_multiple_files = true;
option java_package = "io.glutenproject.extension.proto";

// Extension to carry full output schema for RelRoot
// This preserves exact type information including nullability
// which is critical for correct query execution.
//
// Usage: Pack this message into RelRoot's AdvancedExtension field
// using the URI: https://github.com/oap-project/gluten/extensions/RelRootOutputSchema
message RelRootOutputSchema {
  // The complete output schema as a struct type
  // This includes all type information including nullability
  substrait.Type.Struct output_schema = 1;

  // Reason this was added (for documentation)
  // Example: "Fixes nullable mismatch for aggregation results"
  string reason = 2;
}
```

#### Step 1.2: Update build files

Add to `gluten-core/pom.xml` or `build.gradle` (whichever is used):

```xml
<!-- Add to protobuf compilation sources -->
<protobuf-source-directory>
  src/main/resources/io/glutenproject/proto
</protobuf-source-directory>
```

#### Step 1.3: Generate Java classes

```bash
cd gluten-core
mvn clean compile
# or
gradle clean build

# Verify generated class exists:
# io.glutenproject.extension.proto.RelRootOutputSchema
```

---

### Phase 2: Modify Proto File (15 minutes)

#### Step 2.1: Update algebra.proto

Edit: `gluten-core/src/main/resources/substrait/proto/substrait/algebra.proto`

**Find:**
```proto
message RelRoot {
  Rel input = 1;
  repeated string names = 2;
  Type.Struct output_schema = 3;
}
```

**Replace with:**
```proto
message RelRoot {
  Rel input = 1;
  repeated string names = 2;
  substrait.extensions.AdvancedExtension advanced_extension = 10;
}
```

#### Step 2.2: Regenerate proto classes

```bash
cd gluten-core
mvn clean compile
# Verify RelRoot no longer has getOutputSchema() method
```

---

### Phase 3: Update Java/Scala Code (45-60 minutes)

#### Step 3.1: Update PlanNode.java setter

Edit: `gluten-core/src/main/java/io/glutenproject/substrait/plan/PlanNode.java`

**Find (around line 73-81):**
```java
RelRoot.Builder relRootBuilder = RelRoot.newBuilder();
relRootBuilder.setInput(relNode.toProtobuf());
for (String name : outNames) {
  relRootBuilder.addNames(name);
}
if (outputSchema != null) {
  relRootBuilder.setOutputSchema(outputSchema.toProtobuf().getStruct());
}
planRelBuilder.setRoot(relRootBuilder.build());
```

**Replace with:**
```java
import io.glutenproject.extension.proto.RelRootOutputSchema;
import com.google.protobuf.Any;
import io.substrait.proto.extensions.AdvancedExtension;

// ... in the method:

RelRoot.Builder relRootBuilder = RelRoot.newBuilder();
relRootBuilder.setInput(relNode.toProtobuf());
for (String name : outNames) {
  relRootBuilder.addNames(name);
}
if (outputSchema != null) {
  // Pack output schema into AdvancedExtension
  RelRootOutputSchema extension = RelRootOutputSchema.newBuilder()
      .setOutputSchema(outputSchema.toProtobuf().getStruct())
      .setReason("Preserves nullability for issue-1874")
      .build();

  AdvancedExtension advancedExt = AdvancedExtension.newBuilder()
      .setOptimization(Any.pack(extension))
      .build();

  relRootBuilder.setAdvancedExtension(advancedExt);
}
planRelBuilder.setRoot(relRootBuilder.build());
```

**Note:** We use `setOptimization()` because this extension affects correctness of type handling, which is an optimization concern.

#### Step 3.2: Add helper method (optional but recommended)

Add to `PlanNode.java`:

```java
/**
 * Extracts the output schema from a RelRoot's AdvancedExtension.
 * Returns null if not present.
 */
public static Type.Struct extractOutputSchema(RelRoot relRoot) {
  if (!relRoot.hasAdvancedExtension()) {
    return null;
  }

  AdvancedExtension ext = relRoot.getAdvancedExtension();
  if (!ext.hasOptimization()) {
    return null;
  }

  try {
    if (ext.getOptimization().is(RelRootOutputSchema.class)) {
      RelRootOutputSchema schema = ext.getOptimization().unpack(RelRootOutputSchema.class);
      return schema.getOutputSchema();
    }
  } catch (com.google.protobuf.InvalidProtocolBufferException e) {
    // Log warning and return null
    System.err.println("Failed to unpack RelRootOutputSchema: " + e.getMessage());
  }

  return null;
}
```

---

### Phase 4: Update C++ Code (45-60 minutes)

#### Step 4.1: Update SerializedPlanParser.cpp

Edit: `cpp-ch/local-engine/Parser/SerializedPlanParser.cpp`

**Find (around line 481-505):**
```cpp
// fixes: issue-1874, to keep the nullability as expected.
const auto & output_schema = root_rel.root().output_schema();
if (output_schema.types_size())
{
    auto original_header = query_plan->getCurrentDataStream().header;
    const auto & original_cols = original_header.getColumnsWithTypeAndName();
    if (static_cast<size_t>(output_schema.types_size()) != original_cols.size())
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Mismatch output schema");
    }
    // ... rest of logic
}
```

**Replace with:**
```cpp
#include <io/glutenproject/proto/gluten_substrait_extensions.pb.h>
#include <google/protobuf/any.pb.h>

// ... in the method:

// fixes: issue-1874, to keep the nullability as expected.
std::optional<substrait::Type::Struct> output_schema = std::nullopt;

// Extract output schema from AdvancedExtension if present
if (root_rel.root().has_advanced_extension())
{
    const auto & adv_ext = root_rel.root().advanced_extension();
    if (adv_ext.has_optimization())
    {
        const auto & any_ext = adv_ext.optimization();
        if (any_ext.Is<io::glutenproject::extension::RelRootOutputSchema>())
        {
            io::glutenproject::extension::RelRootOutputSchema schema_ext;
            if (any_ext.UnpackTo(&schema_ext))
            {
                output_schema = schema_ext.output_schema();
            }
        }
    }
}

// Use the extracted schema
if (output_schema.has_value() && output_schema->types_size())
{
    auto original_header = query_plan->getCurrentDataStream().header;
    const auto & original_cols = original_header.getColumnsWithTypeAndName();
    if (static_cast<size_t>(output_schema->types_size()) != original_cols.size())
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Mismatch output schema");
    }
    bool need_final_project = false;
    DB::ColumnsWithTypeAndName final_cols;
    for (int i = 0; i < output_schema->types_size(); ++i)
    {
        const auto & col = original_cols[i];
        auto type = TypeParser::parseType(output_schema->types(i));
        // ... rest of existing logic unchanged
    }
}
```

#### Step 4.2: Update C++ build files

Add to `cpp-ch/local-engine/CMakeLists.txt`:

```cmake
# Add Gluten extension proto compilation
protobuf_generate_cpp(
  GLUTEN_EXT_PROTO_SRCS
  GLUTEN_EXT_PROTO_HDRS
  ${CMAKE_SOURCE_DIR}/../gluten-core/src/main/resources/io/glutenproject/proto/gluten_substrait_extensions.proto
)

# Add to target sources
target_sources(local_engine PRIVATE
  ${GLUTEN_EXT_PROTO_SRCS}
)
```

Or update existing proto compilation to include the new extension proto.

---

### Phase 5: Update Velox Backend (if applicable) (30 minutes)

Check if Velox backend also uses `output_schema`:

```bash
cd cpp/velox
grep -r "output_schema" --include="*.cpp" --include="*.cc" --include="*.h"
```

If found, apply similar changes as in Step 4.1 above.

---

### Phase 6: Testing (45 minutes)

#### Step 6.1: Unit tests

Create: `gluten-core/src/test/scala/io/glutenproject/substrait/RelRootOutputSchemaTest.scala`

```scala
package io.glutenproject.substrait

import io.glutenproject.extension.proto.RelRootOutputSchema
import io.substrait.proto.{RelRoot, Type}
import io.substrait.proto.extensions.AdvancedExtension
import com.google.protobuf.Any
import org.scalatest.funsuite.AnyFunSuite

class RelRootOutputSchemaTest extends AnyFunSuite {

  test("output schema can be packed and unpacked from AdvancedExtension") {
    // Create a simple struct type
    val structType = Type.Struct.newBuilder()
      .addTypes(Type.newBuilder().setBool(Type.Boolean.newBuilder().build()))
      .build()

    // Pack into extension
    val schemaExt = RelRootOutputSchema.newBuilder()
      .setOutputSchema(structType)
      .setReason("Test")
      .build()

    val advExt = AdvancedExtension.newBuilder()
      .setOptimization(Any.pack(schemaExt))
      .build()

    val relRoot = RelRoot.newBuilder()
      .setAdvancedExtension(advExt)
      .build()

    // Unpack and verify
    assert(relRoot.hasAdvancedExtension)
    val unpacked = relRoot.getAdvancedExtension.getOptimization
      .unpack(classOf[RelRootOutputSchema])

    assert(unpacked.getOutputSchema.getTypesCount == 1)
    assert(unpacked.getReason == "Test")
  }
}
```

#### Step 6.2: Integration tests

Run existing Gluten test suites:

```bash
# Scala tests
cd gluten-core
mvn test

# C++ tests (ClickHouse)
cd cpp-ch/local-engine
mkdir build && cd build
cmake ..
make -j
ctest

# E2E Spark tests
cd ../..
./dev/run-tpch-test.sh  # or equivalent
```

**Focus on:**
- Queries with nullable columns
- Aggregation queries (issue-1874 was related to this)
- Window functions
- Complex nested types

#### Step 6.3: Validation checklist

- [ ] All protobuf classes compile without errors
- [ ] Java unit tests pass
- [ ] C++ unit tests pass
- [ ] No change in query results for TPC-H queries
- [ ] Nullable column handling works correctly
- [ ] Aggregation results maintain correct nullability
- [ ] Substrait plan serialization/deserialization works
- [ ] Plan can be round-tripped (serialize → deserialize → serialize)

---

### Phase 7: Documentation (15 minutes)

#### Step 7.1: Update SubstraitModifications.md

Edit: `docs/developers/SubstraitModifications.md`

**Find:**
```markdown
* Added `output_schema` in RelRoot([#1901](https://github.com/oap-project/gluten/pull/1901)).
```

**Replace with:**
```markdown
* ~~Added `output_schema` in RelRoot([#1901](https://github.com/oap-project/gluten/pull/1901)).~~
  * MIGRATED: Now uses AdvancedExtension with `RelRootOutputSchema` (see gluten_substrait_extensions.proto)
```

#### Step 7.2: Update SubstraitDiffAnalysis.md

Edit: `docs/developers/SubstraitDiffAnalysis.md`

Add to the completed migrations section:

```markdown
## Completed Migrations

### ✅ output_schema in RelRoot → AdvancedExtension
- **Date:** [Current Date]
- **Status:** Migrated to `RelRootOutputSchema` extension
- **Breaking Change:** No - backward compatible via extension unpacking
- **Files Changed:**
  - `PlanNode.java`
  - `SerializedPlanParser.cpp`
  - `algebra.proto`
```

---

## Verification Steps

After implementation, verify the migration was successful:

### 1. Proto Diff Check
```bash
cd /tmp
git clone --depth 1 --branch v0.77.0 https://github.com/substrait-io/substrait.git
diff -u substrait/proto/substrait/algebra.proto \
  /home/user/gluten/gluten-core/src/main/resources/substrait/proto/substrait/algebra.proto | grep -A5 -B5 "RelRoot"
```

**Expected:** No difference in RelRoot structure (should match official now)

### 2. Functional Test
```bash
# Run a query with nullable aggregations
spark-sql> SELECT SUM(l_quantity), AVG(l_discount) FROM lineitem WHERE l_shipdate IS NULL;
```

**Expected:** Results should be identical to pre-migration behavior

### 3. Plan Inspection
```scala
// In Spark shell
val df = spark.sql("SELECT COUNT(*) FROM test")
df.queryExecution.executedPlan.asInstanceOf[WholeStageTransformer].nativePlanString()
```

**Expected:** Should see AdvancedExtension in RelRoot instead of direct output_schema field

---

## Rollback Plan

If issues are discovered:

1. **Immediate rollback:**
   ```bash
   git revert <commit-hash>
   mvn clean install
   ```

2. **Keep extension proto:** Don't delete `gluten_substrait_extensions.proto` - it's forward-looking

3. **Report issue:** Document what failed and why

---

## Success Criteria

- [ ] `RelRoot` in `algebra.proto` matches official Substrait v0.77.0
- [ ] All existing tests pass
- [ ] Query results unchanged
- [ ] No performance regression (< 1% overhead acceptable)
- [ ] Code compiles for all backends (ClickHouse, Velox)
- [ ] Documentation updated

---

## Estimated Timeline

| Phase | Duration | Dependencies |
|-------|----------|--------------|
| Phase 1: Extension Definition | 30 min | None |
| Phase 2: Proto Modification | 15 min | Phase 1 complete |
| Phase 3: Java/Scala Code | 60 min | Phase 2 complete |
| Phase 4: C++ Code (ClickHouse) | 60 min | Phase 2 complete |
| Phase 5: C++ Code (Velox) | 30 min | Phase 2 complete |
| Phase 6: Testing | 45 min | Phases 3-5 complete |
| Phase 7: Documentation | 15 min | Phase 6 complete |
| **Total** | **~4 hours** | |

---

## Questions & Troubleshooting

### Q: What if unpacking fails in C++?

A: The code should gracefully handle this by checking `UnpackTo()` return value. If unpacking fails, treat as if no output_schema was provided (existing behavior).

### Q: Should we keep backward compatibility?

A: Yes, for one release cycle. The unpacking code should handle both old plans (with direct field) during a transition period. However, since this is a proto-breaking change, coordination is needed.

### Q: What URI should we use for the extension?

A: Use: `https://github.com/oap-project/gluten/extensions/RelRootOutputSchema`

This follows the pattern: `https://github.com/{org}/{repo}/extensions/{ExtensionName}`

---

## Contact

For questions about this migration:
- Review: `docs/developers/SubstraitModifications.md`
- Issue: Create issue in Gluten repo with tag `substrait-unfork`

---

## Appendix: Complete File Paths

All files that need modification:

```
Modified:
  gluten-core/src/main/resources/substrait/proto/substrait/algebra.proto
  gluten-core/src/main/java/io/glutenproject/substrait/plan/PlanNode.java
  cpp-ch/local-engine/Parser/SerializedPlanParser.cpp
  cpp-ch/local-engine/CMakeLists.txt
  docs/developers/SubstraitModifications.md
  docs/developers/SubstraitDiffAnalysis.md

Created:
  gluten-core/src/main/resources/io/glutenproject/proto/gluten_substrait_extensions.proto
  gluten-core/src/test/scala/io/glutenproject/substrait/RelRootOutputSchemaTest.scala

Optional (if Velox uses it):
  cpp/velox/substrait/SubstraitToVeloxPlan.cc
```
