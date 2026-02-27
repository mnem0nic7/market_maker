use std::path::Path;

use tracing::{debug, warn};

use super::Skill;

/// YAML frontmatter parsed from a skill file.
#[derive(Debug, serde::Deserialize)]
struct SkillFrontmatter {
    name: String,
    #[serde(default)]
    description: String,
}

/// Loads skills from directories by scanning for `*.skill.md` or `SKILL.md` files.
pub struct SkillLoader;

impl SkillLoader {
    /// Load all skills from the given directories.
    pub fn load_from_dirs(dirs: &[String]) -> Vec<Skill> {
        let mut skills = Vec::new();
        for dir in dirs {
            let path = Path::new(dir);
            if !path.is_dir() {
                debug!(dir, "Skill directory not found, skipping");
                continue;
            }
            match std::fs::read_dir(path) {
                Ok(entries) => {
                    for entry in entries.flatten() {
                        let file_path = entry.path();
                        let file_name = file_path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("");

                        if file_name.ends_with(".skill.md") || file_name == "SKILL.md" {
                            match Self::parse_skill_file(&file_path) {
                                Ok(skill) => {
                                    debug!(name = %skill.name, path = %file_path.display(), "Loaded skill");
                                    skills.push(skill);
                                }
                                Err(e) => {
                                    warn!(path = %file_path.display(), error = %e, "Failed to parse skill file");
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(dir, error = %e, "Failed to read skill directory");
                }
            }
        }
        skills
    }

    /// Parse a single skill file with optional YAML frontmatter.
    ///
    /// Format:
    /// ```text
    /// ---
    /// name: my_skill
    /// description: Does something useful
    /// ---
    ///
    /// # Skill Body
    /// Instructions here...
    /// ```
    pub fn parse_skill_file(path: &Path) -> anyhow::Result<Skill> {
        let content = std::fs::read_to_string(path)?;
        let source_path = path.to_string_lossy().to_string();

        if let Some(skill) = Self::parse_with_frontmatter(&content, &source_path) {
            return Ok(skill);
        }

        // No frontmatter — use filename as name
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .trim_end_matches(".skill")
            .to_string();

        Ok(Skill {
            name,
            description: String::new(),
            body: content,
            source_path: Some(source_path),
        })
    }

    /// Try to parse YAML frontmatter between `---` markers.
    fn parse_with_frontmatter(content: &str, source_path: &str) -> Option<Skill> {
        let trimmed = content.trim_start();
        if !trimmed.starts_with("---") {
            return None;
        }

        // Find the closing ---
        let after_first = &trimmed[3..];
        let closing_pos = after_first.find("\n---")?;

        let yaml_str = &after_first[..closing_pos].trim();
        let body_start = closing_pos + 4; // skip past \n---
        let body = after_first[body_start..].trim().to_string();

        let frontmatter: SkillFrontmatter = serde_json::from_value(
            // Use a simple YAML-like parser (key: value per line)
            Self::parse_simple_yaml(yaml_str)?,
        )
        .ok()?;

        Some(Skill {
            name: frontmatter.name,
            description: frontmatter.description,
            body,
            source_path: Some(source_path.to_string()),
        })
    }

    /// Simple YAML-like parser for frontmatter (key: value per line).
    /// Returns a serde_json::Value object, or None if parsing fails.
    fn parse_simple_yaml(yaml: &str) -> Option<serde_json::Value> {
        let mut map = serde_json::Map::new();
        for line in yaml.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line.split_once(':')?;
            let key = key.trim().to_string();
            let value = value.trim().to_string();
            map.insert(key, serde_json::Value::String(value));
        }
        Some(serde_json::Value::Object(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_parse_with_frontmatter() {
        let content = r#"---
name: dcf_valuation
description: Run a discounted cash flow analysis
---

# DCF Valuation Workflow

1. Gather free cash flow data
2. Estimate WACC
3. Project future cash flows
"#;

        let skill =
            SkillLoader::parse_with_frontmatter(content, "skills/dcf.skill.md").unwrap();
        assert_eq!(skill.name, "dcf_valuation");
        assert_eq!(skill.description, "Run a discounted cash flow analysis");
        assert!(skill.body.contains("DCF Valuation Workflow"));
        assert!(skill.body.contains("Gather free cash flow"));
    }

    #[test]
    fn test_parse_without_frontmatter() {
        let content = "# Just a markdown file\n\nWith some content.";

        let result = SkillLoader::parse_with_frontmatter(content, "test.md");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_skill_file_with_frontmatter() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.skill.md");
        let mut file = std::fs::File::create(&file_path).unwrap();
        write!(
            file,
            "---\nname: test_skill\ndescription: A test skill\n---\n\n# Body\nInstructions here."
        )
        .unwrap();

        let skill = SkillLoader::parse_skill_file(&file_path).unwrap();
        assert_eq!(skill.name, "test_skill");
        assert_eq!(skill.description, "A test skill");
        assert!(skill.body.contains("Instructions here"));
        assert!(skill.source_path.is_some());
    }

    #[test]
    fn test_parse_skill_file_without_frontmatter() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("my_analysis.skill.md");
        let mut file = std::fs::File::create(&file_path).unwrap();
        write!(file, "# Analysis\n\nDo the analysis.").unwrap();

        let skill = SkillLoader::parse_skill_file(&file_path).unwrap();
        assert_eq!(skill.name, "my_analysis");
        assert!(skill.body.contains("Do the analysis"));
    }

    #[test]
    fn test_load_from_dirs() {
        let dir = tempfile::tempdir().unwrap();

        // Create a skill file
        let skill_path = dir.path().join("valuation.skill.md");
        let mut f = std::fs::File::create(&skill_path).unwrap();
        write!(
            f,
            "---\nname: valuation\ndescription: Valuation skill\n---\n\n# Steps"
        )
        .unwrap();

        // Create a non-skill file (should be ignored)
        let other_path = dir.path().join("readme.md");
        std::fs::File::create(&other_path).unwrap();

        let skills = SkillLoader::load_from_dirs(&[dir.path().to_string_lossy().to_string()]);
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].name, "valuation");
    }

    #[test]
    fn test_load_from_nonexistent_dir() {
        let skills = SkillLoader::load_from_dirs(&["/nonexistent/dir".to_string()]);
        assert!(skills.is_empty());
    }

    #[test]
    fn test_simple_yaml_parser() {
        let yaml = "name: my_skill\ndescription: Does things\nextra: value";
        let parsed = SkillLoader::parse_simple_yaml(yaml).unwrap();
        assert_eq!(parsed["name"], "my_skill");
        assert_eq!(parsed["description"], "Does things");
        assert_eq!(parsed["extra"], "value");
    }

    #[test]
    fn test_simple_yaml_with_comments() {
        let yaml = "# Comment\nname: test\n\n# Another comment\ndescription: desc";
        let parsed = SkillLoader::parse_simple_yaml(yaml).unwrap();
        assert_eq!(parsed["name"], "test");
        assert_eq!(parsed["description"], "desc");
    }
}
