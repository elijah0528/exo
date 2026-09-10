use super::{DiffLine, FileDiff, Hunk, diff_stats, parse_unified_diff};

const SAMPLE: &str = "diff --git a/src/main.rs b/src/main.rs
index 111..222 100644
--- a/src/main.rs
+++ b/src/main.rs
@@ -10,4 +10,5 @@ fn main() {
 let a = 1;
-let b = 2;
+let b = 3;
+let c = 4;
 println!();
";

#[test]
fn parses_paths_hunks_and_line_numbers() {
    assert_eq!(
        parse_unified_diff(SAMPLE),
        vec![FileDiff {
            path: "src/main.rs".to_string(),
            hunks: vec![Hunk {
                header: "@@ -10,4 +10,5 @@ fn main() {".to_string(),
                lines: vec![
                    DiffLine::Context {
                        old: 10,
                        new: 10,
                        text: "let a = 1;".to_string(),
                    },
                    DiffLine::Removed {
                        old: 11,
                        text: "let b = 2;".to_string(),
                    },
                    DiffLine::Added {
                        new: 11,
                        text: "let b = 3;".to_string(),
                    },
                    DiffLine::Added {
                        new: 12,
                        text: "let c = 4;".to_string(),
                    },
                    DiffLine::Context {
                        old: 12,
                        new: 13,
                        text: "println!();".to_string(),
                    },
                ],
            }],
        }]
    );
}

#[test]
fn counts_additions_and_deletions() {
    assert_eq!(diff_stats(&parse_unified_diff(SAMPLE)), (2, 1));
}

#[test]
fn ignores_output_without_hunks() {
    assert_eq!(
        parse_unified_diff("diff --git a/x b/x\nindex 1..2\n"),
        Vec::new()
    );
}

#[test]
fn parses_multiple_files() {
    let diff = format!(
        "{SAMPLE}diff --git a/b.rs b/b.rs\n--- a/b.rs\n+++ b/b.rs\n@@ -1 +1 @@\n-old\n+new\n"
    );
    let files = parse_unified_diff(&diff);
    assert_eq!(
        files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        vec!["src/main.rs", "b.rs"]
    );
    assert_eq!(diff_stats(&files), (3, 2));
}
