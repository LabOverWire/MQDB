use std::path::PathBuf;

pub(crate) fn cmd_passwd(
    username: &str,
    batch: Option<String>,
    delete: bool,
    file: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    if delete {
        let path = file.ok_or("--file is required for --delete")?;
        let contents = std::fs::read_to_string(&path)?;
        let prefix = format!("{username}:");
        let remaining: Vec<&str> = contents
            .lines()
            .filter(|line| !line.starts_with(&prefix))
            .collect();
        std::fs::write(&path, remaining.join("\n") + "\n")?;
        eprintln!("Deleted user '{username}' from {}", path.display());
        return Ok(());
    }

    let password = if let Some(p) = batch {
        p
    } else {
        eprint!("Password: ");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        input.trim().to_string()
    };

    let hash = mqtt5::broker::auth::PasswordAuthProvider::hash_password(&password)?;

    if let Some(path) = file {
        let mut contents = std::fs::read_to_string(&path).unwrap_or_default();
        let prefix = format!("{username}:");
        let mut found = false;
        let updated: Vec<String> = contents
            .lines()
            .map(|line| {
                if line.starts_with(&prefix) {
                    found = true;
                    format!("{username}:{hash}")
                } else {
                    line.to_string()
                }
            })
            .collect();
        contents = updated.join("\n") + "\n";
        if !found {
            use std::fmt::Write;
            let _ = writeln!(contents, "{username}:{hash}");
        }
        std::fs::write(&path, &contents)?;
        eprintln!(
            "{} user '{username}' in {}",
            if found { "Updated" } else { "Added" },
            path.display()
        );
    } else {
        println!("{username}:{hash}");
    }

    Ok(())
}

pub(crate) fn cmd_scram(
    username: &str,
    batch: Option<String>,
    delete: bool,
    stdout: bool,
    file: Option<PathBuf>,
    iterations: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    use mqtt5::broker::auth_mechanisms::{
        generate_scram_credential_line, generate_scram_credential_line_with_iterations,
    };
    use std::collections::HashMap;

    if username.contains(':') {
        return Err("username cannot contain ':' character".into());
    }
    if iterations < 10000 {
        return Err("iteration count must be at least 10000".into());
    }

    if delete {
        let path = file.ok_or("--file is required for --delete")?;
        let contents = std::fs::read_to_string(&path)?;
        let prefix = format!("{username}:");
        let remaining: Vec<&str> = contents
            .lines()
            .filter(|line| !line.starts_with(&prefix))
            .collect();
        std::fs::write(&path, remaining.join("\n") + "\n")?;
        eprintln!("Deleted user '{username}' from {}", path.display());
        return Ok(());
    }

    let password = if let Some(p) = batch {
        p
    } else {
        eprint!("Password: ");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        input.trim().to_string()
    };

    let line = if iterations == 310_000 {
        generate_scram_credential_line(username, &password)?
    } else {
        generate_scram_credential_line_with_iterations(username, &password, iterations)?
    };

    if stdout || file.is_none() {
        println!("{line}");
        return Ok(());
    }

    let path = file.unwrap();
    let mut users: HashMap<String, String> = HashMap::new();

    if path.exists() {
        let contents = std::fs::read_to_string(&path)?;
        for file_line in contents.lines() {
            let trimmed = file_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some(uname) = trimmed.split(':').next() {
                users.insert(uname.to_string(), trimmed.to_string());
            }
        }
    }

    let action = if users.contains_key(username) {
        "Updated"
    } else {
        "Added"
    };
    users.insert(username.to_string(), line);

    let mut sorted_names: Vec<&String> = users.keys().collect();
    sorted_names.sort();
    let mut output = String::new();
    for name in sorted_names {
        if let Some(l) = users.get(name) {
            output.push_str(l);
            output.push('\n');
        }
    }
    std::fs::write(&path, &output)?;
    eprintln!("{action} user '{username}' in {}", path.display());

    Ok(())
}
