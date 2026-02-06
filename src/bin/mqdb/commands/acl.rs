use std::path::PathBuf;

use crate::AclAction;

pub(crate) async fn cmd_acl(action: AclAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        AclAction::Add {
            username,
            topic,
            permission,
            file,
        } => cmd_acl_add(&username, &topic, &permission, &file).await,
        AclAction::Remove {
            username,
            topic,
            file,
        } => cmd_acl_remove(&username, topic.as_deref(), &file).await,
        AclAction::RoleAdd {
            role_name,
            topic,
            permission,
            file,
        } => cmd_acl_role_add(&role_name, &topic, &permission, &file).await,
        AclAction::RoleRemove {
            role_name,
            topic,
            file,
        } => cmd_acl_role_remove(&role_name, topic.as_deref(), &file).await,
        AclAction::RoleList { role_name, file } => {
            cmd_acl_role_list(role_name.as_deref(), &file).await
        }
        AclAction::Assign {
            username,
            role,
            file,
        } => cmd_acl_assign(&username, &role, &file).await,
        AclAction::Unassign {
            username,
            role,
            file,
        } => cmd_acl_unassign(&username, &role, &file).await,
        AclAction::List { user, file } => cmd_acl_list(user.as_deref(), &file).await,
        AclAction::Check {
            username,
            topic,
            action,
            file,
        } => cmd_acl_check(&username, &topic, &action, &file).await,
        AclAction::UserRoles { username, file } => cmd_acl_user_roles(&username, &file).await,
    }
}

async fn read_acl_lines(path: &PathBuf) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let content = tokio::fs::read_to_string(path).await?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(String::from)
        .collect())
}

async fn write_acl_lines(
    path: &PathBuf,
    lines: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::AsyncWriteExt;
    let mut content = String::new();
    for line in lines {
        content.push_str(line);
        content.push('\n');
    }
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await?;
    file.write_all(content.as_bytes()).await?;
    Ok(())
}

async fn cmd_acl_add(
    username: &str,
    topic: &str,
    permission: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let valid = ["read", "write", "readwrite", "deny"];
    if !valid.contains(&permission) {
        return Err(format!(
            "invalid permission '{permission}': must be read, write, readwrite, or deny"
        )
        .into());
    }
    let mut rules = if file.exists() {
        read_acl_lines(file).await?
    } else {
        Vec::new()
    };
    let rule = format!("user {username} topic {topic} permission {permission}");
    if rules.iter().any(|r| r == &rule) {
        println!("Rule already exists: {rule}");
        return Ok(());
    }
    rules.push(rule.clone());
    write_acl_lines(file, &rules).await?;
    println!("Added ACL rule: {rule}");
    Ok(())
}

async fn cmd_acl_remove(
    username: &str,
    topic: Option<&str>,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rules = read_acl_lines(file).await?;
    let original = rules.len();
    rules.retain(|rule| {
        let parts: Vec<&str> = rule.split_whitespace().collect();
        if parts.len() != 6 || parts[0] != "user" || parts[2] != "topic" || parts[4] != "permission"
        {
            return true;
        }
        if parts[1] != username {
            return true;
        }
        if let Some(t) = topic {
            parts[3] != t
        } else {
            false
        }
    });
    let removed = original - rules.len();
    if removed == 0 {
        return Err(format!("no rules found for user '{username}'").into());
    }
    write_acl_lines(file, &rules).await?;
    println!("Removed {removed} rule(s) for user '{username}'");
    Ok(())
}

async fn cmd_acl_role_add(
    role_name: &str,
    topic: &str,
    permission: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let valid = ["read", "write", "readwrite", "deny"];
    if !valid.contains(&permission) {
        return Err(format!("invalid permission '{permission}'").into());
    }
    let mut rules = if file.exists() {
        read_acl_lines(file).await?
    } else {
        Vec::new()
    };
    let rule = format!("role {role_name} topic {topic} permission {permission}");
    if rules.iter().any(|r| r == &rule) {
        println!("Role rule already exists: {rule}");
        return Ok(());
    }
    rules.push(rule.clone());
    write_acl_lines(file, &rules).await?;
    println!("Added role rule: {rule}");
    Ok(())
}

async fn cmd_acl_role_remove(
    role_name: &str,
    topic: Option<&str>,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rules = read_acl_lines(file).await?;
    let original = rules.len();
    rules.retain(|rule| {
        let parts: Vec<&str> = rule.split_whitespace().collect();
        if parts.len() != 6 || parts[0] != "role" || parts[2] != "topic" || parts[4] != "permission"
        {
            return true;
        }
        if parts[1] != role_name {
            return true;
        }
        if let Some(t) = topic {
            parts[3] != t
        } else {
            false
        }
    });
    let removed = original - rules.len();
    if removed == 0 {
        return Err(format!("no rules found for role '{role_name}'").into());
    }
    write_acl_lines(file, &rules).await?;
    println!("Removed {removed} rule(s) from role '{role_name}'");
    Ok(())
}

async fn cmd_acl_role_list(
    role_name: Option<&str>,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let rules = read_acl_lines(file).await?;
    let filtered: Vec<&String> = rules
        .iter()
        .filter(|r| {
            let parts: Vec<&str> = r.split_whitespace().collect();
            if parts.len() < 2 || parts[0] != "role" {
                return false;
            }
            role_name.is_none_or(|name| parts[1] == name)
        })
        .collect();
    if filtered.is_empty() {
        println!("No role rules found");
    } else {
        for rule in &filtered {
            println!("  {rule}");
        }
        println!("\nTotal: {} rule(s)", filtered.len());
    }
    Ok(())
}

async fn cmd_acl_assign(
    username: &str,
    role_name: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rules = if file.exists() {
        read_acl_lines(file).await?
    } else {
        Vec::new()
    };
    let line = format!("assign {username} {role_name}");
    if rules.iter().any(|r| r == &line) {
        println!("Assignment already exists");
        return Ok(());
    }
    rules.push(line);
    write_acl_lines(file, &rules).await?;
    println!("Assigned role '{role_name}' to user '{username}'");
    Ok(())
}

async fn cmd_acl_unassign(
    username: &str,
    role_name: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rules = read_acl_lines(file).await?;
    let line = format!("assign {username} {role_name}");
    let original = rules.len();
    rules.retain(|r| r != &line);
    if rules.len() == original {
        return Err(
            format!("no assignment found for user '{username}' with role '{role_name}'").into(),
        );
    }
    write_acl_lines(file, &rules).await?;
    println!("Removed role '{role_name}' from user '{username}'");
    Ok(())
}

async fn cmd_acl_list(
    user: Option<&str>,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let rules = read_acl_lines(file).await?;
    let filtered: Vec<&String> = if let Some(u) = user {
        rules
            .iter()
            .filter(|r| {
                let parts: Vec<&str> = r.split_whitespace().collect();
                parts.len() >= 2 && parts[0] == "user" && parts[1] == u
            })
            .collect()
    } else {
        rules.iter().collect()
    };
    if filtered.is_empty() {
        println!("No ACL rules found");
    } else {
        for rule in &filtered {
            println!("  {rule}");
        }
        println!("\nTotal: {} rule(s)", filtered.len());
    }
    Ok(())
}

async fn cmd_acl_check(
    username: &str,
    topic: &str,
    action: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    use mqtt5::broker::acl::AclManager;

    if action != "read" && action != "write" {
        return Err(format!("action must be 'read' or 'write', got: {action}").into());
    }
    let acl_manager = AclManager::from_file(file).await?;
    let allowed = if action == "read" {
        acl_manager.check_subscribe(Some(username), topic).await
    } else {
        acl_manager.check_publish(Some(username), topic).await
    };
    if allowed {
        println!("ALLOWED: user '{username}' can {action} topic '{topic}'");
    } else {
        println!("DENIED: user '{username}' cannot {action} topic '{topic}'");
    }
    Ok(())
}

async fn cmd_acl_user_roles(
    username: &str,
    file: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let lines = read_acl_lines(file).await?;
    let assigned_roles: Vec<&str> = lines
        .iter()
        .filter_map(|r| {
            let parts: Vec<&str> = r.split_whitespace().collect();
            if parts.len() == 3 && parts[0] == "assign" && parts[1] == username {
                Some(parts[2])
            } else {
                None
            }
        })
        .collect();
    if assigned_roles.is_empty() {
        println!("User '{username}' has no assigned roles");
    } else {
        println!("Roles for user '{username}':");
        for role in &assigned_roles {
            println!("  {role}");
        }
    }
    Ok(())
}
