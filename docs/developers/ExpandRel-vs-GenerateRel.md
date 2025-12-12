---
layout: page
title: ExpandRel vs GenerateRel - Should They Be Merged?
nav_order: 14
parent: Developer Overview
---

# ExpandRel vs GenerateRel: Should They Be Merged?

**Question:** Should GenerateRel be merged with Substrait's existing ExpandRel or added as a separate operator?

**Answer:** They should remain **SEPARATE** operators because they have fundamentally different semantics.

---

## Quick Comparison

| Aspect | ExpandRel | GenerateRel |
|--------|-----------|-------------|
| **Purpose** | Duplicate records with fixed expansion | Generate variable rows from collection |
| **Cardinality** | **FIXED** - same count for all inputs | **VARIABLE** - depends on runtime data |
| **Determination** | Known at **plan time** | Known at **execution time** |
| **SQL Use Case** | GROUP BY CUBE/ROLLUP/GROUPING SETS | EXPLODE/UNNEST/LATERAL VIEW |
| **Output Count** | All inputs → N rows (same N) | Each input → 0..M rows (different M) |
| **Extra Column** | `duplicate_id` (int64 ordinal) | Generator-specific (e.g., position) |
| **Example** | CUBE(a,b,c) → 8 rows per group | EXPLODE([1,2,3]) → 3 rows |

---

## Detailed Analysis

### ExpandRel (Substrait v0.77.0)

**Purpose:** "Duplicates records, possibly switching output expressions between each duplicate"

**Proto Definition:**
```proto
message ExpandRel {
  RelCommon common = 1;
  Rel input = 2;
  repeated ExpandField fields = 4;

  message ExpandField {
    oneof field_type {
      SwitchingField switching_field = 2;  // Different value per duplicate
      Expression consistent_field = 3;      // Same value for all duplicates
    }
  }

  message SwitchingField {
    // All switching fields must have SAME duplicate count!
    repeated Expression duplicates = 1;
  }
}
```

**Key Constraint:** "identical numbers of output rows per input row across all inputs"

**Example: GROUP BY CUBE**
```sql
SELECT a, b, SUM(c)
FROM table
GROUP BY CUBE(a, b)
```

**Logical Expansion:**
```
Input: {a: 1, b: 2, c: 10}

Expand into 4 rows (2^2 combinations):
  {a: 1,    b: 2,    c: 10, duplicate_id: 0}  -- (a, b)
  {a: 1,    b: NULL, c: 10, duplicate_id: 1}  -- (a)
  {a: NULL, b: 2,    c: 10, duplicate_id: 2}  -- (b)
  {a: NULL, b: NULL, c: 10, duplicate_id: 3}  -- ()

EVERY input produces exactly 4 rows!
```

**Cardinality:** Fixed at plan time (CUBE with 3 columns → 2^3 = 8 rows)

---

### GenerateRel (Gluten)

**Purpose:** "Applies table-generating function to produce variable output rows"

**Proto Definition:**
```proto
message GenerateRel {
  RelCommon common = 1;
  Rel input = 2;
  Expression generator = 3;              // The table-generating function
  repeated Expression child_output = 4;  // Passthrough columns
  bool outer = 5;                        // Emit NULL for empty
}
```

**Key Characteristic:** Output count determined by **runtime data**

**Example: EXPLODE**
```sql
SELECT id, skill
FROM employees
LATERAL VIEW EXPLODE(skills) AS skill
```

**Variable Expansion:**
```
Input 1: {id: 1, skills: ["Java", "Python", "SQL"]}
Output:  3 rows
  {id: 1, skill: "Java"}
  {id: 1, skill: "Python"}
  {id: 1, skill: "SQL"}

Input 2: {id: 2, skills: ["Go"]}
Output:  1 row
  {id: 2, skill: "Go"}

Input 3: {id: 3, skills: []}
Output:  0 rows (or 1 with NULL if outer=true)

Different input → different output count!
```

**Cardinality:** Unknown until execution (depends on array length)

---

## Fundamental Semantic Difference

### Cardinality Semantics

**ExpandRel:**
- **Uniform expansion:** All inputs produce the **same** number of outputs
- **Plan-time constant:** Output count = 2^N (CUBE) or N+1 (ROLLUP) known at plan time
- **Switching fields constraint:** All must have identical duplicate counts

**GenerateRel:**
- **Variable expansion:** Each input produces **different** number of outputs
- **Runtime value:** Output count = array.length, map.size, etc. (data-dependent)
- **No such constraint:** Generator function determines output count

### Example Demonstrating Incompatibility

Consider trying to use ExpandRel for EXPLODE:

```sql
-- Input table
{id: 1, arr: [10, 20, 30]}
{id: 2, arr: [40]}
{id: 3, arr: []}
```

**With ExpandRel (WRONG):**
```
Problem: How many duplicates?
- If duplicates=3: Row 1 ✓, Row 2 produces [40, NULL, NULL], Row 3 produces [NULL, NULL, NULL]
- If duplicates=1: Row 1 produces only [10], losing data!
- No way to specify "variable count based on array length"
```

**With GenerateRel (CORRECT):**
```
generator: explode(arr)
- Row 1 → 3 rows (10, 20, 30)
- Row 2 → 1 row (40)
- Row 3 → 0 rows (or 1 NULL if outer=true)
```

---

## SQL Operations Comparison

### ExpandRel Use Cases

**1. GROUP BY CUBE**
```sql
-- Fixed expansion: 2^N combinations
GROUP BY CUBE(city, state, country)
-- 8 rows per input group (2^3)
```

**2. GROUP BY ROLLUP**
```sql
-- Fixed expansion: N+1 hierarchical levels
GROUP BY ROLLUP(year, quarter, month)
-- 4 rows per input group (3+1)
```

**3. GROUP BY GROUPING SETS**
```sql
-- Fixed expansion: explicit list
GROUP BY GROUPING SETS ((city, state), (city), ())
-- 3 rows per input group
```

**4. Multiple COUNT DISTINCT**
```sql
-- Spark duplicates input for each COUNT DISTINCT
SELECT COUNT(DISTINCT col1), COUNT(DISTINCT col2)
FROM table
-- 2 rows per input (one per distinct aggregation)
```

### GenerateRel Use Cases

**1. EXPLODE (arrays)**
```sql
-- Variable expansion: array.length
SELECT explode(array_col) FROM table
```

**2. POSEXPLODE (arrays with position)**
```sql
-- Variable expansion: array.length, adds position column
SELECT posexplode(array_col) AS (pos, val) FROM table
```

**3. UNNEST (SQL standard)**
```sql
-- Variable expansion: collection.size
SELECT * FROM table, UNNEST(array_col) AS val
```

**4. EXPLODE (maps)**
```sql
-- Variable expansion: map.size
SELECT explode(map_col) AS (key, value) FROM table
```

**5. JSON_TUPLE**
```sql
-- Variable expansion: extracts N fields
SELECT json_tuple(json_col, 'f1', 'f2') AS (f1, f2)
```

**6. LATERAL VIEW OUTER**
```sql
-- Handles empty collections
SELECT * FROM table LATERAL VIEW OUTER EXPLODE(arr) AS val
-- Empty array → 1 row with NULL
```

---

## Why Merging Would Be Problematic

### Option 1: Extend ExpandRel with Generator Field

```proto
message ExpandRel {
  // ... existing fields ...

  message ExpandField {
    oneof field_type {
      SwitchingField switching_field = 2;
      Expression consistent_field = 3;
      GeneratorField generator_field = 4;  // NEW
    }
  }

  message GeneratorField {
    Expression generator = 1;  // Variable-count generator
  }
}
```

**Problems:**
1. ❌ **Breaks semantic constraint:** "identical numbers of output rows" no longer true
2. ❌ **Confusing semantics:** Mix of fixed and variable cardinality in same operator
3. ❌ **Implementation complexity:** Engines must handle two completely different execution paths
4. ❌ **Incompatible with existing uses:** CUBE/ROLLUP logic assumes fixed count
5. ❌ **Output schema issues:** duplicate_id doesn't make sense for variable expansion

### Option 2: Make SwitchingField Support Variable Length

```proto
message SwitchingField {
  oneof cardinality {
    int32 fixed_count = 1;           // For CUBE/ROLLUP
    Expression variable_count = 2;    // For EXPLODE
  }
  repeated Expression duplicates = 3;
}
```

**Problems:**
1. ❌ **Breaks contract:** "all switching fields must have same duplicate count"
2. ❌ **Complex validation:** How to validate mixed fixed/variable in same ExpandRel?
3. ❌ **Runtime complexity:** Different execution strategies per field
4. ❌ **Unclear semantics:** What does duplicate_id mean with variable count?

### Option 3: Deprecate ExpandRel, Create Unified Operator

```proto
message UnifiedExpandRel {
  oneof expansion_type {
    FixedExpansion fixed = 1;      // For CUBE/ROLLUP
    GeneratorExpansion variable = 2; // For EXPLODE
  }
}
```

**Problems:**
1. ❌ **Breaking change:** Existing ExpandRel consumers break
2. ❌ **Migration cost:** All existing Substrait plans need updating
3. ❌ **Backward incompatibility:** v0.77.0 plans won't work
4. ❌ **No clear benefit:** Just renames the problem

---

## Why Separate Operators Are Better

### 1. **Clear Semantics**
- ExpandRel: "Duplicate with fixed count"
- GenerateRel: "Generate with variable count"
- No confusion about cardinality guarantees

### 2. **Easier Implementation**
```cpp
// ExpandRel execution (simple)
for (int i = 0; i < fixed_duplicate_count; i++) {
  output_row[duplicate_id] = i;
  emit(output_row);
}

// GenerateRel execution (different logic)
Collection collection = evaluate_generator(input);
for (auto element : collection) {
  output_row[generator_column] = element;
  emit(output_row);
}
```

### 3. **Better Validation**
```
ExpandRel: Validate all switching_fields have same duplicate count
GenerateRel: Validate generator returns collection type
```

### 4. **Optimizations**
- **ExpandRel:** Can pre-allocate buffers (known size)
- **GenerateRel:** Needs streaming evaluation (unknown size)

### 5. **SQL Mapping**
- **ExpandRel:** GROUP BY CUBE/ROLLUP/GROUPING SETS, multiple COUNT DISTINCT
- **GenerateRel:** EXPLODE, UNNEST, LATERAL VIEW, table functions

Clear 1:1 mapping between SQL concept and operator.

### 6. **Independent Evolution**
- ExpandRel can add features for aggregation use cases
- GenerateRel can add features for unnesting use cases
- No risk of breaking each other

---

## Precedent in Other Systems

### Apache Calcite (SQL optimizer framework)

**Separate operators:**
- `Aggregate` with `groupingSets` for CUBE/ROLLUP
- `Uncollect` for UNNEST operations

### PostgreSQL

**Different implementations:**
- CUBE/ROLLUP: Implemented via aggregation grouping
- UNNEST: Implemented via set-returning function

### Spark Physical Plan

**Separate operators:**
- `Expand` for CUBE/ROLLUP/GROUPING SETS
- `Generate` for EXPLODE/POSEXPLODE/LATERAL VIEW

**Note:** Spark itself keeps them separate!

---

## Alternative: Shared Base, Different Specializations

If there's concern about "too many operators," consider a shared abstraction:

```proto
// Base abstraction (doesn't need to be in proto)
interface RowExpansionRel {
  Rel input;
  RelCommon common;
}

// Specializations
message ExpandRel implements RowExpansionRel {
  // Fixed cardinality expansion
  repeated ExpandField fields;
}

message GenerateRel implements RowExpansionRel {
  // Variable cardinality expansion
  Expression generator;
  repeated Expression child_output;
  bool outer;
}
```

**Benefits:**
- Documentation can group them as "row expansion operators"
- Common optimizations (e.g., predicate pushdown) can apply to both
- Still maintains clear semantic separation

**Implementation:** Just documentation/tooling, no proto changes needed.

---

## Recommendation

### ✅ Add GenerateRel as Separate Operator

**Reasons:**
1. **Different cardinality semantics** - fixed vs variable is fundamental
2. **Different SQL operations** - CUBE vs EXPLODE are distinct concepts
3. **Clearer semantics** - no confusion about output count guarantees
4. **Easier implementation** - separate execution strategies
5. **Independent evolution** - can add features without interfering
6. **Precedent** - Spark, Calcite keep them separate

### ❌ Don't Merge with ExpandRel

**Reasons:**
1. **Breaking change** - violates ExpandRel's core constraint
2. **Semantic confusion** - mixed fixed/variable cardinality
3. **Implementation complexity** - two execution paths in one operator
4. **No clear benefit** - doesn't simplify anything

---

## Substrait Proposal Approach

When proposing GenerateRel to Substrait community:

### DO:
✅ Acknowledge ExpandRel exists
✅ Explain why GenerateRel addresses different use case
✅ Show cardinality difference is fundamental
✅ Demonstrate widespread SQL precedent (EXPLODE/UNNEST)
✅ Provide clear semantics and examples
✅ Show it's orthogonal to ExpandRel, not overlapping

### DON'T:
❌ Suggest replacing ExpandRel
❌ Claim they solve the same problem
❌ Propose merging them
❌ Criticize ExpandRel design

### Suggested Phrasing:

> "GenerateRel addresses variable-cardinality row generation (EXPLODE/UNNEST), which is complementary to ExpandRel's fixed-cardinality duplication (CUBE/ROLLUP). While both expand rows, they serve different SQL operations with fundamentally different cardinality semantics:
>
> - **ExpandRel:** All inputs produce N outputs (fixed)
> - **GenerateRel:** Each input produces 0..M outputs (variable)
>
> This semantic difference makes them suitable for different use cases and justifies separate operators."

---

## Conclusion

**GenerateRel and ExpandRel should remain separate operators** because:

1. They have **fundamentally different cardinality semantics** (fixed vs variable)
2. They map to **different SQL operations** (CUBE/ROLLUP vs EXPLODE/UNNEST)
3. Merging would **complicate semantics** without clear benefit
4. Keeping them separate provides **clearer semantics and easier implementation**
5. **Precedent exists** in other SQL systems (Spark, Calcite)

Both operators are valuable and serve distinct purposes in the relational algebra.

---

## References

- Substrait ExpandRel: https://github.com/substrait-io/substrait/blob/v0.77.0/proto/substrait/algebra.proto
- Spark Expand operator: Used for GROUP BY CUBE/ROLLUP
- Spark Generate operator: Used for EXPLODE/POSEXPLODE/LATERAL VIEW
- Gluten GenerateRel implementation: `gluten-core/.../GenerateExecTransformer.scala`
- GROUP BY extensions: CUBE, ROLLUP, GROUPING SETS (SQL standard)
- Table-generating functions: EXPLODE, UNNEST (SQL standard)
