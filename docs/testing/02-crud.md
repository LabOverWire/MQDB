# Basic CRUD Operations & Output Formats

[Back to index](README.md)

## 2. Basic CRUD Operations

> **Note:** These examples assume the agent started in Section 1 with `--passwd`. All commands
> require `--user admin --pass admin123` (or set `MQDB_USER`/`MQDB_PASS` env vars).

### Create Entity

**Terminal 2:**
```bash
mqdb create users --data '{"name": "Alice", "email": "alice@example.com", "age": 30}' \
  --user admin --pass admin123
```

**Expected output (agent mode):**
```json
{
  "data": {
    "_version": 1,
    "age": 30,
    "email": "alice@example.com",
    "id": "1",
    "name": "Alice"
  },
  "status": "ok"
}
```

> **Note:** Response format differs between modes. In agent mode, `id` is inside `data`.
> In cluster mode, `id` and `entity` are top-level fields alongside `data`.

### Read Entity

```bash
# Use the ID returned from create
mqdb read users <id>
```

**Expected output:**
```json
{
  "data": {
    "_version": 1,
    "age": 30,
    "email": "alice@example.com",
    "id": "<id>",
    "name": "Alice"
  },
  "status": "ok"
}
```

### Update Entity

```bash
mqdb update users <id> --data '{"age": 25}'
```

**Expected output:**
```json
{
  "data": {
    "_version": 2,
    "age": 25,
    "email": "alice@example.com",
    "id": "<id>",
    "name": "Alice"
  },
  "status": "ok"
}
```

### Delete Entity

```bash
mqdb delete users <id>
```

**Expected output:**
```json
{
  "data": null,
  "status": "ok"
}
```

### Output Formats

**JSON format (default):**
```bash
mqdb read users user-001 --format json
```

**Table format:**
```bash
mqdb read users user-001 --format table
```

**CSV format:**
```bash
mqdb list users --format csv
```

All client commands support `--format json|table|csv`. Default is `json`.

---

## 31. Output Format Examples

All CRUD and list commands support `--format json` (default), `--format table`, and `--format csv`.

### JSON Output (default)

```bash
mqdb list users --format json --user admin --pass admin123
```

```json
[
  {"age": 25, "email": "alice@example.com", "id": "abc123", "name": "Alice"},
  {"age": 30, "email": "bob@example.com", "id": "def456", "name": "Bob"}
]
```

### Table Output

```bash
mqdb list users --format table --user admin --pass admin123
```

```
id       | name  | email              | age
---------+-------+--------------------+----
abc123   | Alice | alice@example.com  | 25
def456   | Bob   | bob@example.com    | 30
```

### CSV Output

```bash
mqdb list users --format csv --user admin --pass admin123
```

```
id,name,email,age
abc123,Alice,alice@example.com,25
def456,Bob,bob@example.com,30
```

### Single Record Formats

```bash
mqdb read users abc123 --format table --user admin --pass admin123
```

Table and CSV formats work for single-record responses too (`read`, `create`, `update`).

### Verification Checklist

- [ ] `--format json` outputs valid JSON array for list, JSON object for single record
- [ ] `--format table` outputs aligned columns with header
- [ ] `--format csv` outputs CSV with header row
- [ ] All three formats work for: list, read, create, update, schema get, constraint list
