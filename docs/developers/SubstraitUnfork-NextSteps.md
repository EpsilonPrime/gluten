---
layout: page
title: Substrait Unfork - Next Steps
nav_order: 12
parent: Developer Overview
---

# Substrait Unfork: Next Steps

**Last Updated:** 2025-12-12
**Completed PRs:** #11277 (ParquetReadOptions), #11278 (output_schema)

---

## 🎯 What to Tackle Next

After completing PR #11277 and #11278, here are your best options ranked by effort/impact:

### ⭐ RECOMMENDED: Upgrade to v0.77.0 First

**Why:** Get 2 free wins (ExpandRel, names in Struct) + better foundation

**Effort:** 6-8 hours
**Impact:** Reduces diff by ~30 lines, eliminates 2 custom modifications

**Steps:**
1. Update Maven/Gradle dependency: `substrait-java` version to 0.77.0
2. Regenerate proto classes
3. Fix any breaking API changes (likely minimal)
4. Run full test suite
5. Verify ExpandRel and Struct.names work without custom proto

**Risk:** Medium - Some API changes expected but well-documented

---

## OR: Continue Incremental Migration

If you prefer smaller changes, tackle in this order:

### 1. Verify JOIN_TYPE Changes (30 minutes) ⚡

**What:** Check if LEFT_SEMI/RIGHT_SEMI are actually custom modifications

**Why:** May not be a real diff - official v0.23.0 might already have them

**Steps:**
```bash
diff /tmp/substrait-official-v0.23.0/proto/substrait/algebra.proto \
     gluten-core/src/main/resources/substrait/proto/substrait/algebra.proto | \
     grep -A10 "enum JoinType"
```

**If they match:** Just document, no migration needed!

---

### 2. Migrate column_types in NamedStruct (2-3 hours) 🔧

**What:** Move partition column markers to AdvancedExtension

**Why:** Clean, isolated change with clear purpose

**File:** `type.proto`

**Current:**
```proto
message NamedStruct {
  repeated string names = 1;
  Type.Struct struct = 2;
  repeated ColumnType column_types = 3;  // CUSTOM
  enum ColumnType {
    NORMAL_COL = 0;
    PARTITION_COL = 1;
  }
}
```

**Migration:**
1. Create `ColumnTypesExtension` in gluten_substrait_extensions.proto
2. Pack into NamedStruct's AdvancedExtension field
3. Update Java/Scala code to pack/unpack
4. Update C++ parsers to read from extension
5. Test with partitioned tables

**Complexity:** Medium - Used in file reading, but well-isolated

**Alternative:** Propose upstreaming to Substrait (good candidate!)

---

### 3. Migrate WindowFunction Metadata (2-3 hours) 🪟

**What:** Move `window_type`, `column_name` to AdvancedExtension

**Files:** `algebra.proto` (WindowFunction message)

**Current:**
```proto
message WindowFunction {
  // ... existing fields ...
  string column_name = 12;        // CUSTOM
  WindowType window_type = 13;    // CUSTOM
}
```

**Migration:**
1. Create `WindowFunctionMetadata` extension
2. Pack into WindowFunction's AdvancedExtension
3. Update ClickHouse WindowRelParser.cpp
4. Update Velox window parsers
5. Test with window queries (ROW_NUMBER, RANK, etc.)

**Complexity:** Medium - Isolated to window function handling

---

### 4. Investigate Nothing Type (3-4 hours) 🔍

**What:** Determine if `Nothing` type is truly needed

**Files:** `type.proto`

**Usage Found:**
- TypeBuilder.java
- NothingNode.java
- TypeParser.cpp
- StructLiteralNode.java
- Various ClickHouse parsers

**Investigation Tasks:**
1. Document all uses of Nothing type
2. Check if nullable semantics can replace it
3. Verify if any queries actually produce Nothing type
4. Test removing it and see what breaks

**Possible Outcomes:**
- Can be removed → Delete it
- Needed for void returns → Migrate to AdvancedExtension
- Has standard alternative → Replace with standard type

---

### 5. Analyze schema Field in FileOrFiles (3-4 hours) 📄

**What:** Determine if `schema` field is redundant with `ReadRel.base_schema`

**Current Usage:**
- `LocalFilesNode.java:142` - Sets schema for each file
- `ExcelTextFormatFile.cpp:74-75` - Reads schema for column names

**Investigation:**
1. Check if ReadRel.base_schema provides same info
2. Verify if schema differs per file or is constant
3. Test removing it for Parquet/ORC (may only be needed for CSV/Text)

**Possible Outcomes:**
- Redundant → Remove it
- Needed for text files only → Move to TextReadOptions extension
- Needed universally → Propose upstreaming

---

## 🎯 Recommended Sequence

### Short Term (Next 2 weeks)
1. ✅ Verify JOIN_TYPE (30 min)
2. 🚀 Upgrade to v0.77.0 (6-8 hours)
3. 🔧 Migrate column_types (2-3 hours)

**Total:** ~10-12 hours
**Diff Reduction:** ~50 lines

### Medium Term (Next month)
4. 🪟 Migrate WindowFunction metadata (2-3 hours)
5. 🔍 Investigate Nothing type (3-4 hours)
6. 📄 Analyze schema field (3-4 hours)

**Total:** ~8-11 hours
**Diff Reduction:** ~20 lines

### Long Term (Next quarter)
7. Propose upstreaming to Substrait: column_types, partition_columns
8. Major migrations: TextReadOptions, JsonReadOptions, WindowRel, GenerateRel
9. Evaluate ddl.proto replacement with WriteRel

**Total:** ~40-60 hours
**Diff Reduction:** ~100+ lines

---

## 📊 Progress Tracker

| Modification | Status | PR/Issue | Effort | Lines |
|--------------|--------|----------|--------|-------|
| ParquetReadOptions | ✅ Done | #11277 | - | -10 |
| output_schema | ✅ Done | #11278 | - | -12 |
| JOIN_TYPE | 🔄 Next | - | 30m | ~0? |
| v0.77.0 Upgrade | 🎯 Recommended | - | 6-8h | -30 |
| column_types | 📋 Queued | - | 2-3h | -8 |
| WindowFunction meta | 📋 Queued | - | 2-3h | -5 |
| Nothing type | 📋 Queued | - | 3-4h | -6 |
| schema field | 📋 Queued | - | 3-4h | -3 |
| Text/JsonReadOptions | ⏳ Later | - | 6-8h | -20 |
| partition_columns | ⏳ Later | - | 4-6h | -10 |
| WindowRel | ⏳ Later | - | 8-12h | -40 |
| GenerateRel | ⏳ Later | - | 8-12h | -35 |
| ddl.proto | ⏳ Later | - | 4-6h | -25 |

**Current Diff:** ~200 lines (down from 262)
**After recommended path:** ~150 lines
**Ultimate goal:** <100 lines or all in AdvancedExtension

---

## 🚦 Decision Points

### Should I upgrade to v0.77.0 first?

**YES if:**
- Want to maximize long-term compatibility
- Can afford 6-8 hour effort upfront
- Want to reduce technical debt significantly

**NO if:**
- Need incremental wins for tracking purposes
- Want to minimize risk per change
- Prefer smaller, more controlled migrations

### Should I upstream features to Substrait?

**Good candidates for upstreaming:**
- ✅ column_types (partition column marking)
- ✅ partition_columns (Hive-style partitioning)
- ✅ GenerateRel (table-generating functions)

**Less likely to be accepted:**
- ❌ Nothing type (may have standard alternative)
- ❌ WindowFunction metadata (may be redundant)
- ❌ ddl.proto (overlaps with WriteRel)

**How to upstream:**
1. Open discussion in substrait-io/substrait
2. Present use case and benefits
3. Provide proto definition and documentation
4. Implement in substrait-java if accepted

---

## 📞 Next Steps Summary

**Immediate (Today):**
1. Read this document
2. Decide: v0.77.0 upgrade OR incremental path
3. Start with JOIN_TYPE verification (30 min quick win)

**This Week:**
- Complete chosen path (either upgrade or column_types migration)
- Update SubstraitDiffAnalysis.md with results

**This Month:**
- Complete 2-3 more incremental migrations
- Propose upstreaming for column_types

**This Quarter:**
- Major migrations (WindowRel, GenerateRel, file format options)
- Get diff below 100 lines

---

## 📚 Related Documents

- [SubstraitDiffAnalysis.md](SubstraitDiffAnalysis.md) - Complete diff analysis
- [SubstraitModifications.md](SubstraitModifications.md) - Historical modifications list
- [MigrationPlan-OutputSchemaToExtension.md](MigrationPlan-OutputSchemaToExtension.md) - Example migration plan

---

## ✅ Success Criteria

- [ ] All modifications either upstreamed or in AdvancedExtension
- [ ] Using official Substrait v0.77.0 (or later)
- [ ] Diff from official < 100 lines
- [ ] All tests passing
- [ ] No performance regression
- [ ] Documentation updated

