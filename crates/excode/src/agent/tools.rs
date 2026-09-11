use executor::ToolDefinition;
use serde_json::json;

pub fn coding_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "shell".to_string(),
            description:
                "Run a shell command in the sandbox. Use this for edits, tests, and inspection."
                    .to_string(),
            parameters: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "command": { "type": "string" },
                    "cwd": { "type": ["string", "null"] }
                },
                "required": ["command", "cwd"]
            }),
        },
        ToolDefinition {
            name: "read_file".to_string(),
            description: "Read a file from the sandbox.".to_string(),
            parameters: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": { "path": { "type": "string" } }, "required": ["path"]
            }),
        },
        ToolDefinition {
            name: "list_files".to_string(),
            description: "List files up to two directory levels below a path.".to_string(),
            parameters: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": { "path": { "type": ["string", "null"] } },
                "required": ["path"]
            }),
        },
    ]
}
