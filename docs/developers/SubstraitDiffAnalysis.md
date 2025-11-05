---
layout: page
title: Substrait Diff Analysis - Gluten vs Official v0.23.0
nav_order: 10
parent: Developer Overview
---

# Substrait Proto Diff Analysis

**Date:** 2025-11-05
**Gluten Base Version:** Substrait v0.23.0 (with custom modifications)
**Official Latest Version:** v0.77.0
**Version Gap:** 54 releases behind

## Executive Summary

This document provides a detailed, line-by-line analysis of the differences between Gluten's forked Substrait proto files and the official Substrait v0.23.0 release. This serves as the baseline for any unfork effort.

### Files Modified

- ✅ **algebra.proto**: **222 lines of diff** - Significant custom modifications
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

#### 1.1 Enhanced ParquetReadOptions (Lines 121-134)
```diff
+      message ParquetReadOptions {
+        bool enable_row_group_maxmin_index = 1;
+      }
```
**Purpose:** Enable row group min/max index for better Parquet filtering
**Impact:** Low - Optional optimization field
**Upstream Status:** Not in v0.77.0

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

#### 1.8 Added output_schema in RelRoot (Lines 418-423)
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

## Migration Priority Matrix

### Critical (Must Address)
- **TextReadOptions/JsonReadOptions** → Use DelimiterSeparatedTextReadOptions or AdvancedExtension
- **column_types in NamedStruct** → Strong candidate for upstreaming to Substrait
- **WindowRel** → Evaluate ConsistentPartitionWindowRel or use AdvancedExtension
- **GenerateRel** → Propose upstreaming or use AdvancedExtension
- **partition_columns in FileOrFiles** → Propose upstreaming or use AdvancedExtension

### Medium (Should Address)
- **output_schema in RelRoot** → May not be needed, or use AdvancedExtension
- **Nothing type** → Evaluate if actually used, may not be needed
- **WindowFunction modifications** → Review if needed in modern Substrait

### Low (Nice to Have)
- **enable_row_group_maxmin_index** → Optional optimization
- **window metadata fields** → Optional metadata
- **ddl.proto** → May be deprecated in favor of WriteRel

---

## Recommended First Step

The **easiest and most impactful first step** is:

### ✅ Document Current Usage

Create an inventory of which Gluten code actually uses each custom field:

```bash
# Search for WindowRel usage
grep -r "WindowRel" --include="*.scala" --include="*.java"

# Search for GenerateRel usage
grep -r "GenerateRel" --include="*.scala" --include="*.java"

# Search for TextReadOptions usage
grep -r "TextReadOptions" --include="*.scala" --include="*.java"
```

This will:
1. Be completely non-invasive (no code changes)
2. Show which features are actually critical vs unused
3. Help prioritize the migration work
4. Take only 1-2 hours
5. Provide concrete data for planning

---

## Next Steps

After documenting current usage:

1. **Short term:** Update SubstraitModifications.md with this detailed analysis
2. **Medium term:** Propose upstreaming high-value features to Substrait community
3. **Long term:** Migrate to official Substrait with AdvancedExtension for non-upstreamed features

---

## References

- Official Substrait v0.23.0: https://github.com/substrait-io/substrait/tree/v0.23.0
- Official Substrait v0.77.0: https://github.com/substrait-io/substrait/tree/v0.77.0
- Gluten SubstraitModifications.md: docs/developers/SubstraitModifications.md
