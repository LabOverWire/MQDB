use mqdb::{Database, OnDeleteAction};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/fk_set_null_example").await?;

    db.add_foreign_key(
        "posts".into(),
        "category_id".into(),
        "categories".into(),
        "id".into(),
        OnDeleteAction::SetNull,
    )
    .await?;

    println!("=== Foreign Key SET NULL ===\n");

    println!("Creating categories...");
    let tech = json!({"name": "Technology"});
    let created_tech = db.create("categories".into(), tech).await?;
    let tech_id = created_tech["id"].as_str().unwrap();

    let sports = json!({"name": "Sports"});
    let created_sports = db.create("categories".into(), sports).await?;
    let sports_id = created_sports["id"].as_str().unwrap();
    println!("✓ Created categories: {}, {}\n", tech_id, sports_id);

    println!("Creating posts with categories...");
    let post1 = json!({"title": "AI Trends", "category_id": tech_id});
    db.create("posts".into(), post1).await?;

    let post2 = json!({"title": "Cloud Computing", "category_id": tech_id});
    db.create("posts".into(), post2).await?;

    let post3 = json!({"title": "World Cup", "category_id": sports_id});
    db.create("posts".into(), post3).await?;
    println!("✓ Created 3 posts\n");

    let posts_before = db.list("posts".into(), vec![], vec![], None, vec![]).await?;
    println!("Before deletion:");
    for post in &posts_before {
        println!("  {} - category: {:?}", post["title"], post["category_id"]);
    }
    println!();

    println!("Deleting Technology category...");
    db.delete("categories".into(), tech_id.to_string()).await?;
    println!("✓ Category deleted\n");

    let posts_after = db.list("posts".into(), vec![], vec![], None, vec![]).await?;
    println!("After deletion:");
    println!("  Total posts: {} (all still exist)", posts_after.len());
    for post in &posts_after {
        println!("  {} - category: {:?}", post["title"], post["category_id"]);
    }
    println!();

    println!("Summary:");
    println!("- SET NULL preserves child records when parent is deleted");
    println!("- Foreign key field is set to null automatically");
    println!("- Useful for optional relationships");
    println!("- Posts can exist without a category");

    Ok(())
}
