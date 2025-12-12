# GenerateRel Usage in Gluten - Concrete Examples

**Summary:** GenerateRel is used to translate Spark's table-generating functions (EXPLODE, POSEXPLODE, JSON_TUPLE) to native backend operations.

---

## Supported Operations

### 1. EXPLODE
**Spark SQL:**
```sql
SELECT id, value
FROM table
LATERAL VIEW EXPLODE(array_col) AS value
```

**What it does:** Expands array into multiple rows (one per element)

**Example from tests:**
```sql
-- From: backends-clickhouse/.../GlutenClickHouseTPCHParquetSuite.scala
SELECT id, explode(array(id, id+1)) FROM range(10)
-- Input: 10 rows with arrays [0,1], [1,2], ..., [9,10]
-- Output: 20 rows (2 per input)

SELECT id, explode(map(id, id+1, id+2, id+3)) FROM range(10)
-- Explode map into key-value pairs
```

### 2. POSEXPLODE
**Spark SQL:**
```sql
SELECT id, pos, value
FROM table
LATERAL VIEW POSEXPLODE(array_col) tx AS pos, value
```

**What it does:** Expands array with position index

**Example from tests:**
```sql
-- Issue #1767
SELECT posexplode(split(data['k'], ',')) tx AS a, b

-- Issue #2492
SELECT posexplode(split(n_comment, ' ')) FROM nation WHERE n_comment IS NULL

-- Basic example
SELECT id, posexplode(array(id, id+1)) FROM range(10)
-- Output includes position: (0, 0, 0), (0, 1, 1), (1, 0, 1), (1, 1, 2), ...

SELECT id, posexplode(map(id, id+1, id+2, id+3)) FROM range(10)
```

### 3. JSON_TUPLE
**Spark SQL:**
```sql
SELECT json_tuple(json_col, 'field1', 'field2') AS (f1, f2)
FROM table
```

**What it does:** Extracts multiple fields from JSON string

**Status:**
- Velox: NOT supported (validation rejects it)
- ClickHouse: Supported via GenerateRel → expression evaluation

### 4. Complex Nested LATERAL VIEWs
**Example from TPC-DS:**
```sql
SELECT *
FROM (
  SELECT n_name, array(n_comment, n_name) AS arr FROM nation
)
LATERAL VIEW EXPLODE(arr) AS a
ORDER BY a
```

**Multiple LATERAL VIEWs:**
```sql
SELECT *
FROM table
LATERAL VIEW EXPLODE(set) AS b
-- Can have multiple chained lateral views
```

---

## Backend Implementations

### ClickHouse Backend

**Translation:** GenerateRel → ClickHouse ARRAY JOIN

**Code flow:**
1. `GenerateExecTransformer.scala` creates Substrait GenerateRel
2. C++ `ProjectRelParser::parseOp()` receives GenerateRel
3. If generator is `explode/posexplode`:
   - Converts `arrayJoin()` function to ARRAY JOIN step
   - Splits into 3 phases: pre-projection → ARRAY JOIN → post-projection
   - Uses `ArrayJoinStep` to apply max_block_size (prevents OOM)
4. If generator is `json_tuple`:
   - Uses regular expression evaluation (no ARRAY JOIN)

**Why ARRAY JOIN?**
- Avoids OOM when multiple lateral views are chained
- Applies proper block size limits
- Native ClickHouse operation

**Key file:** `cpp-ch/local-engine/Parser/ProjectRelParser.cpp:116-186`

### Velox Backend

**Translation:** GenerateRel → Velox UNNEST

**Supported:**
- ✅ `explode(array)` - Full support
- ❌ `explode(map)` - NOT supported (MAP datatype limitation)
- ❌ `posexplode` - NOT supported (TODO to add)
- ❌ `json_tuple` - NOT supported
- ❌ `outer=true` - NOT supported (LATERAL VIEW OUTER)

**Code:** `backends-velox/.../ValidatorApiImpl.scala::doGeneratorValidate`

**Note:** Velox backend requires inserting a projection node before GenerateRel (see line 115-134 in GenerateExecTransformer.scala)

---

## Substrait Proto Structure

```proto
message GenerateRel {
  RelCommon common = 1;
  Rel input = 2;                        // Input relation
  Expression generator = 3;              // The table-generating function
  repeated Expression child_output = 4;  // Columns to pass through from input
  bool outer = 5;                        // If true, emit NULL when generator returns empty
  AdvancedExtension advanced_extension = 10;
}
```

### Example: explode(array_col)

**Input row:**
```
{id: 1, name: "Alice", skills: ["Java", "Python", "SQL"]}
```

**GenerateRel fields:**
```proto
generator: explode(field_reference(skills))
child_output: [field_reference(id), field_reference(name)]
outer: false
```

**Output rows:**
```
{id: 1, name: "Alice", skill: "Java"}
{id: 1, name: "Alice", skill: "Python"}
{id: 1, name: "Alice", skill: "SQL"}
```

### Example: posexplode(array_col)

**Same input, but generator:**
```proto
generator: posexplode(field_reference(skills))
```

**Output rows:**
```
{id: 1, name: "Alice", pos: 0, skill: "Java"}
{id: 1, name: "Alice", pos: 1, skill: "Python"}
{id: 1, name: "Alice", pos: 2, skill: "SQL"}
```

### Example: LATERAL VIEW OUTER

**Input row with empty array:**
```
{id: 2, name: "Bob", skills: []}
```

**GenerateRel with outer=true:**
```proto
generator: explode(field_reference(skills))
child_output: [field_reference(id), field_reference(name)]
outer: true  // KEY DIFFERENCE
```

**Output row:**
```
{id: 2, name: "Bob", skill: NULL}
```

**Without outer=true:** Would produce 0 rows!

---

## Real-World Use Cases in Gluten

### 1. TPC-DS Queries
Multiple TPC-DS queries use lateral view explode for set operations and array expansion.

### 2. Data Normalization
Converting nested arrays into flat tables:
```sql
-- Nested: {user_id: 1, purchase_dates: ["2024-01-01", "2024-02-15", "2024-03-20"]}
-- Flat:   (1, "2024-01-01"), (1, "2024-02-15"), (1, "2024-03-20")
```

### 3. Text Processing
```sql
SELECT word, COUNT(*)
FROM documents
LATERAL VIEW EXPLODE(SPLIT(content, ' ')) AS word
GROUP BY word
-- Word frequency analysis
```

### 4. Complex Nested Data
```sql
SELECT id, POSEXPLODE(SPLIT(json_data['tags'], ',')) AS (pos, tag)
FROM events
-- Extract and enumerate tags from JSON
```

### 5. Issue Fixes
- **Issue #1767:** POSEXPLODE with split and map access
- **Issue #2492:** POSEXPLODE with NULL handling
- **Issue #2454:** Various explode/posexplode edge cases
- OOM prevention with multiple chained lateral views

---

## Why GenerateRel is Critical

### For Spark Compatibility
- EXPLODE/POSEXPLODE are **fundamental** Spark operations
- Used extensively in:
  - Data engineering pipelines
  - ETL transformations
  - TPC-DS benchmark queries
  - Real-world production workloads

### For Other SQL Engines
- PostgreSQL: `UNNEST(array_col)`
- Presto/Trino: `CROSS JOIN UNNEST(array_col)`
- DuckDB: `UNNEST(array_col)`
- ClickHouse: `ARRAY JOIN array_col`
- BigQuery: `UNNEST(array_col)`

**All major SQL engines have this feature!**

### Current Situation
- ❌ Does NOT exist in Substrait v0.77.0
- ❌ No alternative way to represent variable-cardinality row generation
- ✅ ExpandRel exists but only for **fixed** cardinality (e.g., GROUP BY CUBE)
- ❌ Without GenerateRel, cannot represent common SQL queries

---

## Impact of Missing GenerateRel in Substrait

### For Gluten
- Forces proto fork (this diff)
- Prevents using official Substrait
- Incompatible with other Substrait consumers

### For Substrait Ecosystem
- **Spark** queries cannot be represented
- **Presto/Trino** UNNEST queries cannot be represented
- **DuckDB** UNNEST queries cannot be represented
- **PostgreSQL** UNNEST queries cannot be represented

### For Projects Wanting Substrait Support
- DataFusion: Cannot add UNNEST without this
- Velox: Has UNNEST but no standard Substrait representation
- Arrow: Cannot support array expansion operations

---

## Recommendation

**Propose GenerateRel to Substrait immediately** because:

1. **Universal need:** ALL major SQL engines have this
2. **No alternative:** ExpandRel doesn't solve variable cardinality
3. **Working implementation:** Gluten has proven design
4. **High impact:** Unlocks Spark, Presto, DuckDB, PostgreSQL support
5. **Fundamental operation:** As basic as JOIN or FILTER

**Next Step:** Use the proposal in `GenerateRel-UpstreamProposal.md` to open GitHub issue in substrait-io/substrait.

---

## Files to Review

**Scala (Gluten):**
- `gluten-core/src/main/scala/io/glutenproject/execution/GenerateExecTransformer.scala`
- `gluten-core/src/main/java/io/glutenproject/substrait/rel/GenerateRelNode.java`
- `backends-velox/src/main/scala/io/glutenproject/backendsapi/velox/ValidatorApiImpl.scala`

**C++ (ClickHouse):**
- `cpp-ch/local-engine/Parser/ProjectRelParser.cpp` (lines 116-186)
- `cpp-ch/local-engine/Parser/SerializedPlanParser.cpp` (parseArrayJoinWithDAG)

**Tests:**
- `backends-clickhouse/src/test/scala/io/glutenproject/execution/GlutenClickHouseTPCHParquetSuite.scala`
  - Lines with explode/posexplode tests
  - Issues #1767, #2492, #2454

**Proto:**
- `gluten-core/src/main/resources/substrait/proto/substrait/algebra.proto:1255`
