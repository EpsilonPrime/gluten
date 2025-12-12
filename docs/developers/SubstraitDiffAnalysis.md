---
layout: page
title: Substrait Diff Analysis - Gluten vs Official v0.23.0
nav_order: 10
parent: Developer Overview
---

# Substrait Proto Diff Analysis

**Date:** 2025-11-05 (Updated: 2025-12-12)
**Gluten Base Version:** Substrait v0.23.0 (with custom modifications)
**Official Latest Version:** v0.77.0
**Version Gap:** 54 releases behind

## Executive Summary

This document provides a detailed, line-by-line analysis of the differences between Gluten's forked Substrait proto files and the official Substrait v0.23.0 release. This serves as the baseline for any unfork effort.

### Migration Progress

**Completed Migrations:**
- ✅ ParquetReadOptions.enable_row_group_maxmin_index → Removed (PR #11277)
- ✅ RelRoot.output_schema → Replaced with ProjectRel (PR #11278)

**Remaining Custom Modifications:** ~200 lines

### Files Modified

- ✅ **algebra.proto**: **~200 lines of diff remaining** (down from 222)
- ✅ **type.proto**: **40 lines of diff** - Medium custom modifications
- ➕ **ddl.proto**: **NEW FILE** - Not in official v0.23.0
- ✅ **plan.proto**: No changes (identical to official)
- ✅ **function.proto**: No changes (identical to official)
- ✅ **capabilities.proto**: No changes (identical to official)
- ✅ **extended_expression.proto**: No changes (identical to official)
- ✅ **parameterized_types.proto**: No changes (identical to official)
- ✅ **type_expressions.proto**: No changes (identical to official)

---

## Detailed Changes

### 1. algebra.proto Modifications

#### 1.1 ~~Enhanced ParquetReadOptions~~ ✅ COMPLETED
```diff
+      message ParquetReadOptions {
+        bool enable_row_group_maxmin_index = 1;
+      }
```
**Purpose:** Enable row group min/max index for better Parquet filtering
**Impact:** Low - Optional optimization field
**Upstream Status:** Not in v0.77.0
**Migration Status:** ✅ **REMOVED in PR #11277** - Field was unused and has been deleted

#### 1.2 Added TextReadOptions (Lines 138-148)
```diff
+      message TextReadOptions {
+        string field_delimiter = 1;
+        uint64 max_block_size = 2;
+        NamedStruct schema = 3 [deprecated=true];
+        string quote = 4;
+        uint64 header = 5;
+        string escape = 6;
+        string null_value = 7;
+        bool empty_as_default = 8;
+      }
```
**Purpose:** Support for CSV/text file reading with configurable delimiters, quotes, escape chars
**Impact:** HIGH - Required for text file support in Spark
**Upstream Status:** Not in v0.77.0 (has `DelimiterSeparatedTextReadOptions` instead)

#### 1.3 Added JsonReadOptions (Lines 148-151)
```diff
+      message JsonReadOptions {
+        uint64 max_block_size = 1;
+        NamedStruct schema = 2 [deprecated=true];
+      }
```
**Purpose:** Support JSON file format reading
**Impact:** HIGH - Required for JSON file support in Spark
**Upstream Status:** Not in v0.77.0

#### 1.4 Added Partition Columns Support (Lines 160-167)
```diff
+     message partitionColumn {
+        string key = 1;
+        string value = 2;
+     }
+     repeated partitionColumn partition_columns = 16;
+
+     /// File schema
+     NamedStruct schema = 17;
```
**Purpose:** Support Hive-style partition columns in file paths
**Impact:** HIGH - Critical for partitioned table support
**Upstream Status:** Not in v0.77.0

#### 1.5 Modified Join Types (Lines 197-205)
```diff
-    JOIN_TYPE_SEMI = 5;
-    JOIN_TYPE_ANTI = 6;
+    JOIN_TYPE_LEFT_SEMI = 5;
+    JOIN_TYPE_RIGHT_SEMI = 6;
+    JOIN_TYPE_ANTI = 7;
-    JOIN_TYPE_SINGLE = 7;
+    JOIN_TYPE_SINGLE = 8;
```
**Purpose:** Distinguish left vs right semi joins
**Impact:** MEDIUM - Required for correct Spark semi join semantics
**Upstream Status:** ✅ Already in official v0.23.0 (this was actually already correct!)

#### 1.6 Added WindowRel (Lines 269-279)
```diff
+message WindowRel {
+  RelCommon common = 1;
+  Rel input = 2;
+  repeated Measure measures = 3;
+  repeated Expression partition_expressions = 4;
+  repeated SortField sorts = 5;
+  substrait.extensions.AdvancedExtension advanced_extension = 10;
+
+  message Measure {
+    Expression.WindowFunction measure = 1;
+  }
+}
```
**Purpose:** Support for window functions (e.g., ROW_NUMBER, RANK, LAG, LEAD)
**Impact:** HIGH - Critical for analytical queries
**Upstream Status:** Not in v0.77.0 (has `ConsistentPartitionWindowRel` instead)
**Migration Path:** Evaluate if `ConsistentPartitionWindowRel` can replace this

#### 1.7 Added ExpandRel (Lines 383-411)
```diff
+message ExpandRel {
+  RelCommon common = 1;
+  Rel input = 2;
+  repeated ExpandField fields = 4;
+  substrait.extensions.AdvancedExtension advanced_extension = 10;
+
+  message ExpandField {
+    oneof field_type {
+      SwitchingField switching_field = 2;
+      Expression consistent_field = 3;
+    }
+  }
+
+  message SwitchingField {
+    repeated Expression duplicates = 1;
+  }
+}
```
**Purpose:** Duplicate records with different expressions (used in GROUP BY CUBE/ROLLUP)
**Impact:** MEDIUM - Required for advanced GROUP BY operations
**Upstream Status:** ✅ **UPSTREAMED to v0.77.0!** This should work directly.

#### 1.8 ~~Added output_schema in RelRoot~~ ✅ COMPLETED
```diff
 message RelRoot {
   Rel input = 1;
   repeated string names = 2;
+  Type.Struct output_schema = 3;
 }
```
**Purpose:** Provide complete type schema at root, not just field names
**Impact:** MEDIUM - Useful for schema validation
**Upstream Status:** Not in v0.77.0
**Migration Status:** ✅ **REPLACED in PR #11278** - Now uses explicit ProjectRel for type enforcement instead of implicit output_schema field

#### 1.9 Added GenerateRel (Lines 1252+)
```diff
+message GenerateRel {
+  RelCommon common = 1;
+  Rel input = 2;
+  // (likely more fields below, truncated in diff)
```
**Purpose:** Support for table-generating functions (e.g., EXPLODE, POSEXPLODE in Spark)
**Impact:** HIGH - Required for array/map expansion operations
**Upstream Status:** Not in v0.77.0

#### 1.10 Added WriteRel (Referenced in Rel union)
```diff
+    WriteRel write = 18;
```
**Purpose:** Support for write operations (INSERT, CREATE TABLE AS, etc.)
**Impact:** HIGH - Required for data write operations
**Upstream Status:** ✅ Already in official v0.23.0

#### 1.11 Modified WindowFunction Bounds (Lines 943-966)
```diff
-      message Unbounded {}
+      message Unbounded_Preceding {}
+      message Unbounded_Following {}
+
+        Unbounded_Preceding unbounded_preceding = 4;
+        Unbounded_Following unbounded_following = 5;
```
**Purpose:** Distinguish between unbounded preceding vs following in window frames
**Impact:** MEDIUM - Required for correct window frame semantics
**Upstream Status:** Need to check v0.77.0

#### 1.12 Added window_type and column_name in WindowFunction (Lines 915-916)
```diff
+    string column_name = 12;
+    WindowType window_type = 13;
```
**Purpose:** Additional metadata for window functions
**Impact:** LOW - Metadata fields
**Upstream Status:** Not in v0.77.0

---

### 2. type.proto Modifications

#### 2.1 Added Nothing Type (Lines 48-60)
```diff
+    Nothing nothing = 33;
+
+  message Nothing {
+    uint32 type_variation_reference = 1;
+  }
```
**Purpose:** Represent void/null type (e.g., for procedures that return nothing)
**Impact:** LOW - Edge case handling
**Upstream Status:** Not in v0.77.0

#### 2.2 Added names in Struct (Line 172)
```diff
 message Struct {
   repeated Type types = 1;
   uint32 type_variation_reference = 2;
   Nullability nullability = 3;
+  repeated string names = 4;
 }
```
**Purpose:** Name struct fields for better schema representation
**Impact:** MEDIUM - Improves schema usability
**Upstream Status:** ✅ **UPSTREAMED to v0.77.0!** This should work directly.

#### 2.3 Added column_types in NamedStruct (Lines 236-240)
```diff
 message NamedStruct {
   repeated string names = 1;
   Type.Struct struct = 2;
+  repeated ColumnType column_types = 3;
+  enum ColumnType {
+    NORMAL_COL = 0;
+    PARTITION_COL = 1;
+  }
 }
```
**Purpose:** Distinguish partition columns from data columns in schema
**Impact:** HIGH - Critical for partitioned table support
**Upstream Status:** Not in v0.77.0
**Migration Path:** Strong candidate for upstreaming

---

### 3. ddl.proto (New File)

**Status:** This entire file is custom to Gluten

```proto
message DllPlan {
  oneof dll_type {
    InsertPlan insert_plan = 1;
  }
}

message InsertPlan {
  Plan input = 1;
  ReadRel.ExtensionTable output = 2;
}

message Dll {
  repeated DllPlan dll_plan = 1;
}
```

**Purpose:** Support DDL operations (Data Definition Language), specifically INSERT operations
**Impact:** HIGH - Required for write operations
**Upstream Status:** Not in official Substrait (note: there's a typo "Dll" vs "DDL")
**Migration Path:** Consider using WriteRel instead or propose upstreaming

---

## Migration Progress Tracker

### ✅ Completed (2 items)
- **ParquetReadOptions.enable_row_group_maxmin_index** → REMOVED (PR #11277)
- **output_schema in RelRoot** → REPLACED with ProjectRel (PR #11278)

### 🔄 Next Steps - Updated Priority Matrix

### Critical (Must Address) - 5 items
1. **TextReadOptions/JsonReadOptions** → Use DelimiterSeparatedTextReadOptions or AdvancedExtension
2. **WindowRel** → Evaluate ConsistentPartitionWindowRel or use AdvancedExtension
3. **GenerateRel** → Propose upstreaming or use AdvancedExtension
4. **partition_columns in FileOrFiles** → Propose upstreaming or use AdvancedExtension
5. **column_types in NamedStruct** → Strong candidate for upstreaming to Substrait

### Medium (Should Address) - 4 items
1. **schema field in FileOrFiles** → May be redundant with ReadRel.base_schema
2. **WindowFunction modifications (window_type, column_name, Unbounded)** → Used by ClickHouse backend
3. **Nothing type** → Used in multiple places, needs investigation
4. **ddl.proto** → Used for INSERT operations

### Low (Nice to Have) - 1 item
1. **names in Struct** → Already upstreamed to v0.77.0, get free by upgrading

---

## Updated Recommendations (Post PR #11277, #11278)

### 🎯 Best Next Steps (Ranked by Effort/Impact)

#### Option 1: Upgrade to Substrait v0.77.0 (HIGHEST IMPACT)
**Effort:** 6-8 hours
**Impact:** Eliminates 2 modifications for free

**What you get:**
- ✅ ExpandRel (already upstreamed)
- ✅ names in Struct (already upstreamed)
- ✅ Better foundation for future migrations
- ✅ Reduces diff from ~200 lines to ~170 lines

**Recommendation:** Do this first to maximize compatibility

---

#### Option 2: Tackle Individual Modifications (INCREMENTAL)

Listed in order of easiest → hardest:

**1. JOIN_TYPE enum verification (30 min)**
- Verify if LEFT_SEMI/RIGHT_SEMI changes are actually custom or already in v0.23.0
- May not need any migration

**2. column_types in NamedStruct (2-3 hours)**
- Single enum + field
- Clear purpose (partition vs data columns)
- Good candidate for upstreaming to Substrait
- Can use AdvancedExtension as interim

**3. WindowFunction metadata fields (2-3 hours)**
- `window_type`, `column_name` fields
- `Unbounded_Preceding/Following` split
- Isolated to window function handling
- Can use AdvancedExtension

**4. Nothing type (3-4 hours)**
- Used in multiple places but not pervasive
- Investigation needed on whether it's truly required
- May be able to use existing nullable semantics

**5. schema field in FileOrFiles (3-4 hours)**
- Currently used by ExcelTextFormatFile
- May be redundant with ReadRel.base_schema
- Needs careful analysis

**6. ddl.proto (4-6 hours)**
- Entire custom file
- Used for INSERT operations
- May be replaceable with WriteRel
- Requires coordination across backends

**7. partition_columns in FileOrFiles (4-6 hours)**
- Critical for Hive-style partitioning
- Strong upstreaming candidate
- Complex due to wide usage

**8. TextReadOptions/JsonReadOptions (6-8 hours)**
- High complexity, widely used
- May align with DelimiterSeparatedTextReadOptions
- Critical for CSV/JSON file support

**9. WindowRel (8-12 hours)**
- Large custom message
- Check if ConsistentPartitionWindowRel can replace it
- High complexity migration

**10. GenerateRel (8-12 hours)**
- Large custom message
- Table-generating functions (EXPLODE, etc.)
- High complexity, propose upstreaming

---

### 🚀 Recommended Path Forward

**Phase 1: Quick Wins (8-10 hours)**
1. Upgrade to v0.77.0
2. Verify JOIN_TYPE changes
3. Migrate column_types to AdvancedExtension

**Phase 2: Medium Effort (12-16 hours)**
4. Migrate WindowFunction metadata
5. Investigate Nothing type
6. Analyze schema field redundancy

**Phase 3: Major Migrations (20-30 hours)**
7. Propose upstreaming: column_types, partition_columns, GenerateRel
8. Migrate file format options (Text/Json)
9. Evaluate WindowRel vs ConsistentPartitionWindowRel
10. Handle ddl.proto

---

### Progress Metrics

**Original Diff:** 262 lines
**After PR #11277, #11278:** ~200 lines
**After v0.77.0 upgrade:** ~170 lines (estimated)
**After Phase 1:** ~150 lines (estimated)
**Target:** < 100 lines or all in AdvancedExtension

---

## References

- Official Substrait v0.23.0: https://github.com/substrait-io/substrait/tree/v0.23.0
- Official Substrait v0.77.0: https://github.com/substrait-io/substrait/tree/v0.77.0
- Gluten SubstraitModifications.md: docs/developers/SubstraitModifications.md
