# Backup and Restore

[Back to index](README.md)

Manual tests for backup creation and restore. Assumes an agent started per 01-setup.

## 8. Backup and Restore

### Create Backup

```bash
mqdb backup create
mqdb backup create --name my-snapshot
```

The `--name` flag is optional (default: `backup`). The backup is written to a directory named
exactly `<name>` (no timestamp is appended).

**Expected output:**
```
backup created: my-snapshot
```

### List Backups

```bash
mqdb backup list
```

**Expected output:**
```
Available backups:
  backup
  my-snapshot
```

An empty backup directory prints `No backups found`.

### Restore from Backup

Restore runs offline against the agent's database directory. Stop the agent first.

```bash
mqdb restore --db ./data/testdb --name my-snapshot
```

**Expected output:**
```
restored backup my-snapshot into ./data/testdb (N files)
```

Start the agent again on the same `--db`; records match the time of the backup and every backup
is still listed. With the agent still running, restore fails with
`database at ./data/testdb is in use: stop the agent before restoring` and changes nothing.
A cluster node database is refused. The `$DB/_admin/restore` endpoint is not served online.

`mqdb backup create` and `mqdb backup list` connect to the broker (support `--broker`, `--user`,
`--pass`, `--insecure` flags) and exit non-zero when the broker returns an error.

### Verification Checklist

- [ ] `mqdb backup create --name my-snapshot` prints `backup created: my-snapshot`
- [ ] A `my-snapshot` directory exists under the agent's backup directory
- [ ] `mqdb backup list` includes `my-snapshot`
- [ ] `mqdb backup list` on a fresh agent prints `No backups found`
- [ ] `mqdb backup create --name my-snapshot` a second time succeeds and the backup holds no nested `backups` directory
- [ ] `mqdb restore --db <db> --name my-snapshot` with the agent running fails with "in use" and exits non-zero
- [ ] `mqdb restore --db <db> --name my-snapshot` with the agent stopped brings back the records from the backup
