---
layout: page
title: Substrait Unfork - Next Steps
nav_order: 12
parent: Developer Overview
---

# Substrait Unfork: Next Steps

**Last Updated:** 2025-12-12
**Completed PRs:** #11277 (ParquetReadOptions), #11278 (output_schema), JOIN_TYPE (already correct)

---

## ⚠️ Important Clarifications (UPDATED)

**Key Insight:** Not all custom modifications should be migrated to AdvancedExtension!

Some modifications are **anti-patterns** that violate Substrait design principles:
- ❌ `column_types` in NamedStruct - Wrong approach, should use `partition_columns` instead
- ❌ `column_name` in WindowFunction - Inappropriate metadata, should use field references

**These must be REMOVED and Gluten FIXED**, not migrated to extensions.

**What's correct:**
- ✅ `partition_columns` in FileOrFiles - This is the RIGHT way (keep it!)
- ✅ JOIN_TYPE changes - Already handled correctly
- ✅ Using ProjectRel for type enforcement (PR #11278)

---

## 🎯 What to Tackle Next

After completing PR #11277 and #11278, here are the correct next steps:

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

### 1. ✅ JOIN_TYPE Changes - Already Correct

**Status:** No action needed - already handled correctly

---

### 2. Fix column_types Anti-Pattern (4-6 hours) 🔧

**What:** Remove `column_types` from NamedStruct and fix Gluten properly

**Why:** This violates Substrait design principles - partition columns should be handled differently

**File:** `type.proto`

**Current (WRONG):**
```proto
message NamedStruct {
  repeated string names = 1;
  Type.Struct struct = 2;
  repeated ColumnType column_types = 3;  // ANTI-PATTERN - REMOVE
  enum ColumnType {
    NORMAL_COL = 0;
    PARTITION_COL = 1;
  }
}
```

**Correct Approach:**
- Partition columns should be in `partition_columns` field of FileOrFiles (already exists!)
- NamedStruct should only describe the schema structure
- Read path should differentiate based on FileOrFiles metadata, not NamedStruct

**Migration:**
1. Audit all uses of `column_types` in Gluten
2. Refactor to use `partition_columns` from FileOrFiles instead
3. Remove `column_types` from type.proto
4. Update C++ parsers to read partition info from correct location
5. Test with partitioned Parquet/ORC tables

**Complexity:** Medium-High - Requires refactoring column handling logic

---

### 3. Fix WindowFunction Metadata Anti-Pattern (3-4 hours) 🪟

**What:** Remove `column_name` from WindowFunction and fix window handling

**Why:** Column names don't belong in function metadata - this is inappropriate use of Substrait

**Files:** `algebra.proto` (WindowFunction message)

**Current (WRONG):**
```proto
message WindowFunction {
  // ... existing fields ...
  string column_name = 12;        // ANTI-PATTERN - REMOVE
  WindowType window_type = 13;    // May need review
}
```

**Correct Approach:**
- Column names come from field references in the window expression
- WindowFunction should only describe the function itself
- Window result columns defined by WindowRel, not individual functions

**Migration:**
1. Audit uses of `column_name` in WindowRelParser.cpp and elsewhere
2. Refactor to derive column names from proper sources (WindowRel.measures, output mapping, etc.)
3. Remove `column_name` from proto
4. Review if `window_type` is also redundant
5. Test window queries (ROW_NUMBER, RANK, LAG, LEAD)

**Complexity:** Medium - Requires understanding window function output naming

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

## 🎯 Corrected Categorization

### 🔧 Anti-Patterns to Fix (Remove + Fix Gluten)
These violate Substrait principles and must be removed:

1. **column_types in NamedStruct** (4-6 hours)
   - Use `partition_columns` in FileOrFiles instead
   - Remove from proto, fix Gluten column handling

2. **column_name in WindowFunction** (3-4 hours)
   - Derive from proper field references
   - Remove from proto, fix window naming logic

### 📦 Legitimate Features (Migrate or Upstream)
These are valid needs, candidate for AdvancedExtension or upstreaming:

3. **TextReadOptions/JsonReadOptions** (6-8 hours)
   - Valid file format support
   - Could align with DelimiterSeparatedTextReadOptions
   - Or propose JSON format support upstream

4. **partition_columns in FileOrFiles** (Already exists!)
   - This is actually the RIGHT way to handle partitions
   - Keep this, use it properly instead of column_types

5. **WindowRel** (8-12 hours)
   - Check if ConsistentPartitionWindowRel in v0.77.0 can replace
   - If not, keep as Gluten extension or propose upstream

6. **GenerateRel** (8-12 hours)
   - Table-generating functions (EXPLODE, etc.)
   - Strong candidate for upstreaming
   - Critical Spark feature

### 🤔 Needs Investigation
These may not be needed at all:

7. **Nothing type** (3-4 hours)
   - Investigate if truly needed
   - May be replaceable with standard nullable semantics

8. **schema field in FileOrFiles** (3-4 hours)
   - May be redundant with ReadRel.base_schema
   - Or needed only for specific formats

9. **window_type in WindowFunction** (2-3 hours)
   - Investigate if redundant
   - May be derivable from window spec

10. **ddl.proto** (4-6 hours)
    - May be replaceable with WriteRel
    - Note: "Dll" typo suggests this was hastily added

### ⬆️ Free Wins from v0.77.0 Upgrade
11. **ExpandRel** - Already upstreamed
12. **names in Struct** - Already upstreamed
13. **Unbounded_Preceding/Following** - May already be fixed

---

## 🚀 Recommended Sequence (CORRECTED)

### Phase 1: Foundation (6-8 hours)
1. 🚀 **Upgrade to v0.77.0 first**
   - Get ExpandRel, Struct.names for free
   - See what else is already fixed
   - Better foundation for all other work

### Phase 2: Fix Anti-Patterns (7-10 hours)
2. 🔧 **Fix column_types** - Remove and use partition_columns properly
3. 🪟 **Fix column_name in WindowFunction** - Use proper field references

### Phase 3: Investigate Questionable Fields (8-12 hours)
4. 🔍 **Investigate Nothing type** - Can it be removed?
5. 📄 **Analyze schema field** - Is it redundant?
6. 🪟 **Review window_type** - Is it needed?

### Phase 4: Migrate Legitimate Features (20-30 hours)
7. 📦 **TextReadOptions/JsonReadOptions** - Migrate or upstream
8. 📦 **WindowRel** - Evaluate vs ConsistentPartitionWindowRel
9. 📦 **GenerateRel** - Propose upstreaming
10. 📦 **ddl.proto** - Migrate or replace with WriteRel

**Total Estimated Effort:** 40-60 hours
**Target:** All modifications either removed or in AdvancedExtension

---

## 📊 Progress Tracker

| Modification | Category | Status | PR/Issue | Effort | Lines |
|--------------|----------|--------|----------|--------|-------|
| ParquetReadOptions | Removed | ✅ Done | #11277 | - | -10 |
| output_schema | Fixed | ✅ Done | #11278 | - | -12 |
| JOIN_TYPE | Correct | ✅ Done | - | - | 0 |
| v0.77.0 Upgrade | Foundation | 🎯 Next | - | 6-8h | -30 |
| column_types | Anti-pattern | 🔧 Fix | - | 4-6h | -8 |
| column_name (Window) | Anti-pattern | 🔧 Fix | - | 3-4h | -3 |
| Nothing type | Investigate | 🔍 TBD | - | 3-4h | -6 |
| schema field | Investigate | 🔍 TBD | - | 3-4h | -3 |
| window_type | Investigate | 🔍 TBD | - | 2-3h | -2 |
| Text/JsonReadOptions | Legitimate | 📦 Migrate | - | 6-8h | -20 |
| partition_columns | Legitimate | ✅ Keep | - | 0h | 0 |
| WindowRel | Legitimate | 📦 Evaluate | - | 8-12h | -40 |
| GenerateRel | Legitimate | 📦 Upstream | - | 8-12h | -35 |
| ddl.proto | Investigate | 🔍 TBD | - | 4-6h | -25 |
| Unbounded split | Check v0.77.0 | ⬆️ Maybe free | - | 0h | -3 |

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

### Should I fix anti-patterns or migrate them?

**Anti-patterns - MUST FIX, not migrate:**
- ❌ column_types - Use partition_columns instead
- ❌ column_name in WindowFunction - Use proper field references

**DON'T migrate these to AdvancedExtension - they're design violations!**

### Should I upstream features to Substrait?

**Good candidates for upstreaming:**
- ✅ GenerateRel (table-generating functions) - Critical Spark feature
- ✅ TextReadOptions/JsonReadOptions - Common file formats
- 🤔 partition_columns (if not already upstream-able)

**Wrong candidates (anti-patterns):**
- ❌ column_types - This is the WRONG approach, don't propose
- ❌ column_name in WindowFunction - Violates Substrait design

**Investigate first:**
- 🤔 Nothing type - May not be needed
- 🤔 WindowRel - Check if ConsistentPartitionWindowRel already solves it
- 🤔 ddl.proto - Likely overlaps with WriteRel

**How to upstream:**
1. Open discussion in substrait-io/substrait
2. Present use case and benefits
3. Provide proto definition and documentation
4. Implement in substrait-java if accepted

---

## 📞 Next Steps Summary

**Immediate (This Week):**
1. 🚀 Upgrade to v0.77.0 (foundation for everything)
2. 🔧 Fix column_types anti-pattern (use partition_columns properly)

**This Month:**
- 🪟 Fix column_name in WindowFunction anti-pattern
- 🔍 Investigate Nothing type, schema field, window_type
- Update SubstraitDiffAnalysis.md with results

**This Quarter:**
- 📦 Migrate legitimate features (TextReadOptions, JsonReadOptions)
- 📦 Evaluate WindowRel vs ConsistentPartitionWindowRel
- 📦 Propose upstreaming GenerateRel
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

