# Backup and Restore

[Back to index](README.md)

## 8. Backup and Restore

### Create Backup

```bash
mqdb backup create
mqdb backup create --name my-snapshot
```

The `--name` flag is optional (default: `backup`).

### List Backups

```bash
mqdb backup list
```

### Restore from Backup

Restore sends a restore command to the running broker via MQTT. The agent handles the restore internally — no restart required.

```bash
mqdb restore --name backup_20241208_143022
```

All backup/restore commands connect to the broker (support `--broker`, `--user`, `--pass`, `--insecure` flags).
