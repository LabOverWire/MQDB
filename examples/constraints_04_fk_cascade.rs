use mqdb::{Database, OnDeleteAction};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/fk_cascade_example").await?;

    db.add_foreign_key(
        "posts".into(),
        "author_id".into(),
        "users".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await?;

    db.add_foreign_key(
        "comments".into(),
        "post_id".into(),
        "posts".into(),
        "id".into(),
        OnDeleteAction::Cascade,
    )
    .await?;

    println!("=== Foreign Key CASCADE Delete ===\n");

    println!("Creating user...");
    let user = json!({"name": "Alice"});
    let created_user = db.create("users".into(), user).await?;
    let user_id = created_user["id"].as_str().unwrap();
    println!("✓ Created user: {}\n", user_id);

    println!("Creating 2 posts by this user...");
    let post1 = json!({"title": "First Post", "author_id": user_id});
    let created_post1 = db.create("posts".into(), post1).await?;
    let post1_id = created_post1["id"].as_str().unwrap();

    let post2 = json!({"title": "Second Post", "author_id": user_id});
    let created_post2 = db.create("posts".into(), post2).await?;
    let post2_id = created_post2["id"].as_str().unwrap();
    println!("✓ Created posts: {}, {}\n", post1_id, post2_id);

    println!("Creating 3 comments on posts...");
    db.create("comments".into(), json!({"text": "Nice!", "post_id": post1_id}))
        .await?;
    db.create("comments".into(), json!({"text": "Great!", "post_id": post1_id}))
        .await?;
    db.create("comments".into(), json!({"text": "Awesome!", "post_id": post2_id}))
        .await?;
    println!("✓ Created 3 comments\n");

    let posts_before = db.list("posts".into(), vec![], vec![], None, vec![], None).await?;
    let comments_before = db.list("comments".into(), vec![], vec![], None, vec![], None).await?;
    println!("Before deletion:");
    println!("  Posts: {}", posts_before.len());
    println!("  Comments: {}\n", comments_before.len());

    println!("Deleting user (should cascade to posts and comments)...");
    db.delete("users".into(), user_id.to_string()).await?;
    println!("✓ User deleted\n");

    let posts_after = db.list("posts".into(), vec![], vec![], None, vec![], None).await?;
    let comments_after = db.list("comments".into(), vec![], vec![], None, vec![], None).await?;
    println!("After deletion:");
    println!("  Posts: {} (cascaded)", posts_after.len());
    println!("  Comments: {} (cascaded from posts)\n", comments_after.len());

    println!("Summary:");
    println!("- CASCADE deletes all referencing entities automatically");
    println!("- Works recursively (User → Posts → Comments)");
    println!("- Maintains referential integrity");
    println!("- All deletions happen atomically in one transaction");

    Ok(())
}
