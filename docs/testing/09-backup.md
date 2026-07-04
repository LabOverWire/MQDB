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

```bash
mqdb restore --name my-snapshot
```

> **Note:** In agent mode the `$DB/_admin/restore` endpoint is not handled online — it returns
> `Error: restore requires agent restart - use CLI with --restore flag`. Restore a snapshot by
> stopping the agent and starting it against the restored data directory.

All backup/restore commands connect to the broker (support `--broker`, `--user`, `--pass`, `--insecure` flags).

### Verification Checklist

- [ ] `mqdb backup create --name my-snapshot` prints `backup created: my-snapshot`
- [ ] A `my-snapshot` directory exists under the agent's backup directory
- [ ] `mqdb backup list` includes `my-snapshot`
- [ ] `mqdb backup list` on a fresh agent prints `No backups found`
- [ ] `mqdb restore --name my-snapshot` reports the restart-required error in agent mode
