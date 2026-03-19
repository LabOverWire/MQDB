# Filtering, Sorting, Pagination, Limits, Edge Cases & MQTT-Only Features

[Back to index](README.md)

## 3. List and Filtering

### Setup Test Data

```bash
mqdb create products --data '{"name": "Laptop", "price": 999, "category": "electronics", "stock": 50}'
mqdb create products --data '{"name": "Mouse", "price": 29, "category": "electronics", "stock": 200}'
mqdb create products --data '{"name": "Desk", "price": 299, "category": "furniture", "stock": 25}'
mqdb create products --data '{"name": "Chair", "price": 199, "category": "furniture", "stock": 0}'
mqdb create products --data '{"name": "Keyboard", "price": 79, "category": "electronics", "stock": 150}'
```

> **Note:** IDs are auto-generated if not provided. To use a client-provided ID, include `"id"` in the payload:
> `mqdb create products --data '{"id": "my-uuid", "name": "Monitor", "price": 499}'`
> If no `"id"` field is present, the server generates a partition-prefixed hex ID (e.g., `"6fc263177a320176-0011"`).
>
> **Important:** In agent mode, `create` with an existing ID performs an upsert (overwrites the existing record).
> To prevent duplicates with client-provided IDs, add a unique constraint on the relevant field.

### Basic List

```bash
mqdb list products
```

### Equality Filter (=)

```bash
mqdb list products --filter 'category=electronics'
```

**Expected:** Returns Laptop, Mouse, Keyboard

### Not Equal Filter (<>)

```bash
mqdb list products --filter 'category<>electronics'
```

**Expected:** Returns Desk, Chair

> **Note:** Use `<>` (SQL-style) for not-equal. Use single quotes around the filter expression
> to prevent shell interpretation of special characters.

### Greater Than Filter (>)

```bash
mqdb list products --filter 'price>100'
```

**Expected:** Returns Laptop, Desk, Chair

### Less Than Filter (<)

```bash
mqdb list products --filter 'price<100'
```

**Expected:** Returns Mouse, Keyboard

### Greater Than or Equal (>=)

```bash
mqdb list products --filter 'price>=199'
```

**Expected:** Returns Laptop, Desk, Chair

### Less Than or Equal (<=)

```bash
mqdb list products --filter 'stock<=25'
```

**Expected:** Returns Desk, Chair

### Like/Pattern Filter (~)

```bash
mqdb list products --filter 'name~*board*'
```

**Expected:** Returns Keyboard

### Is Null Filter (?)

```bash
mqdb list products --filter 'description?'
```

**Expected:** Returns all products (none have description field)

### Is Not Null Filter (!?)

```bash
mqdb list products --filter "price!?"
```

> **Shell warning:** Use double quotes for `!?` filters. In bash/zsh, `!?` inside single
> quotes can trigger history expansion, silently corrupting the filter string.

**Expected:** Returns all products (all have price field)

### Sorting

**Ascending:**
```bash
mqdb list products --sort price:asc
```

**Descending:**
```bash
mqdb list products --sort price:desc
```

**Multiple sort fields (comma-separated):**
```bash
mqdb list products --sort 'category:asc,price:desc'
```

### Pagination

```bash
mqdb list products --limit 2
mqdb list products --limit 2 --offset 2
```

### Combined Filters

```bash
mqdb list products --filter 'category=electronics' --filter 'price<100' --sort price:asc
```

**Expected:** Returns Mouse, Keyboard sorted by price

---

## 32. Query Limits and Enforcement

MQDB enforces hard limits on query complexity: `MAX_FILTERS=16`, `MAX_SORT_FIELDS=4`, and `MAX_LIST_RESULTS=10,000`.

### Setup

```bash
mqdb agent start --db /tmp/mqdb-limits-test --bind 127.0.0.1:1883 --passwd /tmp/passwd.txt --admin-users admin
```

Create test entity:
```bash
mqdb create products --data '{"name":"Widget","price":10,"category":"tools"}' --user admin --pass admin123
```

### 17 Filters Returns Error

```bash
mqdb list products \
    --filter 'name=a' --filter 'name=b' --filter 'name=c' --filter 'name=d' \
    --filter 'name=e' --filter 'name=f' --filter 'name=g' --filter 'name=h' \
    --filter 'name=i' --filter 'name=j' --filter 'name=k' --filter 'name=l' \
    --filter 'name=m' --filter 'name=n' --filter 'name=o' --filter 'name=p' \
    --filter 'name=q' \
    --user admin --pass admin123
```

**Expected:** Error response with `"validation error: too many filters: 17 exceeds maximum of 16"`

### 5 Sort Fields Returns Error

```bash
mqdb list products --sort 'a:asc,b:asc,c:asc,d:asc,e:asc' --user admin --pass admin123
```

**Expected:** Error response with `"validation error: too many sort fields: 5 exceeds maximum of 4"`

### 16 Filters Succeeds (Boundary)

```bash
mqdb list products \
    --filter 'name=a' --filter 'name=b' --filter 'name=c' --filter 'name=d' \
    --filter 'name=e' --filter 'name=f' --filter 'name=g' --filter 'name=h' \
    --filter 'name=i' --filter 'name=j' --filter 'name=k' --filter 'name=l' \
    --filter 'name=m' --filter 'name=n' --filter 'name=o' --filter 'name=p' \
    --user admin --pass admin123
```

**Expected:** Empty result set `[]` (no records match all 16 filters), but no error — the query is accepted.

### 4 Sort Fields Succeeds (Boundary)

```bash
mqdb list products --sort 'name:asc,price:desc,category:asc,id:desc' --user admin --pass admin123
```

**Expected:** Products listed sorted by the 4-field sort key. No error.

### Large Result Set Truncation

> **Note:** This test requires creating >10,000 records. The broker silently truncates results to 10,000 items and logs the truncation server-side.

```bash
# Create 10,001 records using a loop
for i in $(seq 1 10001); do
    mqdb create items --data "{\"n\":$i}" --user admin --pass admin123 > /dev/null
done

# List all
mqdb list items --user admin --pass admin123 | python3 -c "import sys,json; print(len(json.load(sys.stdin)))"
```

**Expected:** Output is `10000` — the result is silently truncated.

### Cluster Mode

Repeat the 17-filter and 5-sort-field tests from a non-primary node (dev cluster uses credentials `admin`/`admin`):

```bash
mqdb dev start-cluster --nodes 3 --clean
sleep 10
mqdb create products --data '{"name":"Widget"}' --user admin --pass admin --broker 127.0.0.1:1884

mqdb list products \
    --filter 'name=a' --filter 'name=b' --filter 'name=c' --filter 'name=d' \
    --filter 'name=e' --filter 'name=f' --filter 'name=g' --filter 'name=h' \
    --filter 'name=i' --filter 'name=j' --filter 'name=k' --filter 'name=l' \
    --filter 'name=m' --filter 'name=n' --filter 'name=o' --filter 'name=p' \
    --filter 'name=q' \
    --user admin --pass admin --broker 127.0.0.1:1885
```

**Expected:** Error `"validation error: too many filters: 17 exceeds maximum of 16"` from any node (same format as agent mode).

### Verification Checklist

- [ ] 17 filters returns 400 error with correct message
- [ ] 5 sort fields returns 400 error with correct message
- [ ] 16 filters succeeds (boundary)
- [ ] 4 sort fields succeeds (boundary)
- [ ] >10,000 results silently truncated to 10,000
- [ ] Limit errors reproduced from non-primary cluster node

---

## 33. Sort Edge Cases

### Setup

```bash
mqdb agent start --db /tmp/mqdb-sort-test --bind 127.0.0.1:1883 --passwd /tmp/passwd.txt --admin-users admin
```

### Sort on Missing Field

Create records where some lack the sort field:

```bash
mqdb create items --data '{"name":"Alpha","priority":1}' --user admin --pass admin123
mqdb create items --data '{"name":"Beta","priority":3}' --user admin --pass admin123
mqdb create items --data '{"name":"Gamma"}' --user admin --pass admin123
mqdb create items --data '{"name":"Delta","priority":2}' --user admin --pass admin123
mqdb create items --data '{"name":"Epsilon"}' --user admin --pass admin123
```

```bash
mqdb list items --sort 'priority:asc' --user admin --pass admin123
```

**Expected:** Records missing `priority` sort to the FRONT (ascending). Order: Gamma, Epsilon (no priority), then Alpha (1), Delta (2), Beta (3).

```bash
mqdb list items --sort 'priority:desc' --user admin --pass admin123
```

**Expected:** Records missing `priority` sort to the BACK (descending). Order: Beta (3), Delta (2), Alpha (1), then Gamma, Epsilon (no priority).

### Sort on Mixed Types

Create records with a field that has both number and string values:

```bash
mqdb create mixed --data '{"name":"A","value":100}' --user admin --pass admin123
mqdb create mixed --data '{"name":"B","value":"hello"}' --user admin --pass admin123
mqdb create mixed --data '{"name":"C","value":5}' --user admin --pass admin123
```

```bash
mqdb list mixed --sort 'value:asc' --user admin --pass admin123
```

**Expected:** All records returned. Ordering depends on JSON value comparison (numbers before strings, or vice versa). Verify the output is deterministic.

### Multi-Field Sort

```bash
mqdb create employees --data '{"dept":"Engineering","name":"Charlie","salary":90}' --user admin --pass admin123
mqdb create employees --data '{"dept":"Engineering","name":"Alice","salary":120}' --user admin --pass admin123
mqdb create employees --data '{"dept":"Sales","name":"Bob","salary":80}' --user admin --pass admin123
mqdb create employees --data '{"dept":"Engineering","name":"Bob","salary":100}' --user admin --pass admin123
mqdb create employees --data '{"dept":"Sales","name":"Alice","salary":95}' --user admin --pass admin123
```

```bash
mqdb list employees --sort 'dept:asc,name:asc' --user admin --pass admin123
```

**Expected:** Primary sort by `dept`, secondary sort by `name` within each department:
1. Engineering, Alice
2. Engineering, Bob
3. Engineering, Charlie
4. Sales, Alice
5. Sales, Bob

### Sort with All Null Values

All records missing the sort field:

```bash
mqdb create nullsort --data '{"name":"A"}' --user admin --pass admin123
mqdb create nullsort --data '{"name":"B"}' --user admin --pass admin123
mqdb create nullsort --data '{"name":"C"}' --user admin --pass admin123
```

```bash
mqdb list nullsort --sort 'missing_field:asc' --user admin --pass admin123
```

**Expected:** All 3 records returned. Order is stable (all are equal for the sort key).

### Verification Checklist

- [ ] Records missing sort field sort to front (ascending)
- [ ] Records missing sort field sort to back (descending)
- [ ] Mixed-type sort returns all records deterministically
- [ ] Multi-field sort applies correct priority (primary then secondary)
- [ ] Sort on universally missing field returns all records

---

## 34. Filter Edge Cases

### Setup

```bash
mqdb agent start --db /tmp/mqdb-filter-test --bind 127.0.0.1:1883 --passwd /tmp/passwd.txt --admin-users admin
```

### Filter on Nonexistent Field Without Schema

No schema set — filter on a field no record has:

```bash
mqdb create products --data '{"name":"Widget","price":10}' --user admin --pass admin123
mqdb create products --data '{"name":"Gadget","price":25}' --user admin --pass admin123

mqdb list products --filter 'color=red' --user admin --pass admin123
```

**Expected:** Empty result set `[]` — no records have `color`, so none match. No error because there is no schema to validate against.

### Filter on Nonexistent Field With Schema

Set a schema, then filter on a field not in it:

```bash
echo '{"name":{"type":"string"},"price":{"type":"number"}}' > /tmp/widgets_schema.json
mqdb schema set widgets --file /tmp/widgets_schema.json --user admin --pass admin123
mqdb create widgets --data '{"name":"Sprocket","price":15}' --user admin --pass admin123

mqdb list widgets --filter 'color=red' --user admin --pass admin123
```

**Expected:** Error: `"schema validation failed: color - filter field does not exist in schema"`

### Sort on Nonexistent Field With Schema

```bash
mqdb list widgets --sort 'color:asc' --user admin --pass admin123
```

**Expected:** Error: `"schema validation failed: color - sort field does not exist in schema"`

### Projection on Nonexistent Field With Schema

```bash
mqdb list widgets --projection color --user admin --pass admin123
```

**Expected:** Error: `"schema validation failed: color - projection field does not exist in schema"`

### Multiple Filters on Same Field (Range Narrowing)

```bash
mqdb create prices --data '{"item":"A","price":30}' --user admin --pass admin123
mqdb create prices --data '{"item":"B","price":75}' --user admin --pass admin123
mqdb create prices --data '{"item":"C","price":150}' --user admin --pass admin123
mqdb create prices --data '{"item":"D","price":200}' --user admin --pass admin123

mqdb list prices --filter 'price>50' --filter 'price<200' --user admin --pass admin123
```

**Expected:** Only item B (75) and C (150) returned — both filters narrow the range.

### Contradictory Filters

```bash
mqdb list prices --filter 'price>100' --filter 'price<50' --user admin --pass admin123
```

**Expected:** Empty result set `[]` — no value can be both >100 and <50.

### Filter with Empty String

```bash
mqdb create tags --data '{"label":"important"}' --user admin --pass admin123
mqdb create tags --data '{"label":""}' --user admin --pass admin123
mqdb create tags --data '{"label":"urgent"}' --user admin --pass admin123

mqdb list tags --filter 'label=' --user admin --pass admin123
```

**Expected:** Only the record with `"label":""` is returned.

### Like Filter with No Wildcards

```bash
mqdb list tags --filter 'label~important' --user admin --pass admin123
```

**Expected:** Only the record with `"label":"important"` is returned — like filter without `*` wildcards matches the exact value.

### Null Filter Combined with Equality

> **Note:** The `?` (is null) filter matches records where the field exists with an explicit JSON `null` value. Records that simply omit the field are NOT matched — a missing field is treated as "doesn't match any filter", not as null.

```bash
mqdb create inventory --data '{"category":"electronics","description":"A phone"}' --user admin --pass admin123
mqdb create inventory --data '{"category":"electronics","description":null}' --user admin --pass admin123
mqdb create inventory --data '{"category":"furniture","description":"A table"}' --user admin --pass admin123
mqdb create inventory --data '{"category":"furniture","description":null}' --user admin --pass admin123

mqdb list inventory --filter 'category=electronics' --filter 'description?' --user admin --pass admin123
```

**Expected:** Only the electronics record with `"description": null` is returned — `?` matches explicit null values, not missing fields.

### Verification Checklist

- [ ] Filter on nonexistent field without schema returns empty set
- [ ] Filter on nonexistent field with schema returns 400 error
- [ ] Sort on nonexistent field with schema returns 400 error
- [ ] Projection on nonexistent field with schema returns 400 error
- [ ] Multiple filters on same field narrow results correctly
- [ ] Contradictory filters return empty set
- [ ] Filter with empty string matches empty-string records
- [ ] Like filter without wildcards matches exact value
- [ ] Null filter matches explicit null, not missing fields

---

## 35. MQTT-Only Query Features

These features are accessible only via MQTT payloads, not through CLI `--filter` syntax.

### Setup

```bash
mqdb agent start --db /tmp/mqdb-mqtt-query-test --bind 127.0.0.1:1883 --passwd /tmp/passwd.txt --admin-users admin
```

### In Operator

The `in` operator matches records where a field's value is in a given array. This is only available via direct MQTT payload.

```bash
mqdb create products --data '{"name":"Phone","category":"electronics"}' --user admin --pass admin123
mqdb create products --data '{"name":"Laptop","category":"electronics"}' --user admin --pass admin123
mqdb create products --data '{"name":"Desk","category":"furniture"}' --user admin --pass admin123
mqdb create products --data '{"name":"Shirt","category":"clothing"}' --user admin --pass admin123
mqdb create products --data '{"name":"Chair","category":"furniture"}' --user admin --pass admin123
```

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/products/list' -e 'resp/test' -W 5 \
    -m '{"filters":[{"field":"category","op":"in","value":["electronics","furniture"]}]}'
```

**Expected:** 4 records returned (Phone, Laptop, Desk, Chair) — clothing is excluded.

### In Operator with Empty Array

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/products/list' -e 'resp/test' -W 5 \
    -m '{"filters":[{"field":"category","op":"in","value":[]}]}'
```

**Expected:** Empty result set — no value is contained in an empty array.

### In Operator Combined with Other Filters

```bash
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/products/list' -e 'resp/test' -W 5 \
    -m '{"filters":[{"field":"category","op":"in","value":["electronics","furniture"]},{"field":"name","op":"eq","value":"Desk"}]}'
```

**Expected:** Only the Desk record returned — it matches both the `in` filter (furniture) and the `eq` filter (name=Desk).

### Includes (Relationships)

Includes load related entities via foreign key relationships. Requires a foreign key constraint between entities.

```bash
# Create parent entity
mqdb create authors --data '{"id":"author-1","name":"Alice"}' --user admin --pass admin123
mqdb create authors --data '{"id":"author-2","name":"Bob"}' --user admin --pass admin123

# Create child entity with FK
mqdb create posts --data '{"title":"First Post","author_id":"author-1"}' --user admin --pass admin123
mqdb create posts --data '{"title":"Second Post","author_id":"author-2"}' --user admin --pass admin123
mqdb create posts --data '{"title":"Third Post","author_id":"author-1"}' --user admin --pass admin123

# Add foreign key constraint (creates the relationship)
mqdb constraint add posts --fk 'author_id:authors:id' --user admin --pass admin123

# List posts with included author
mosquitto_rr -h 127.0.0.1 -p 1883 -u admin -P admin123 \
    -t '$DB/posts/list' -e 'resp/test' -W 5 \
    -m '{"includes":["authors"]}'
```

**Expected:** Each post includes its related author data nested in the response.

### Verification Checklist

- [ ] `in` operator via MQTT payload matches records in value array
- [ ] `in` operator with empty array returns empty set
- [ ] `in` operator combined with `eq` filter narrows correctly
- [ ] Includes load related entities via FK relationships
