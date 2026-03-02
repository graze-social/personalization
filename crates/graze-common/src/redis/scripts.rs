//! Lua script management for Redis.

use std::collections::HashMap;
use std::path::Path;

use deadpool_redis::redis::{self, Script};
use deadpool_redis::Connection;
use parking_lot::RwLock;
use tracing::{info, warn};

use crate::error::{GrazeError, Result};

/// Path to Lua scripts directory.
const LUA_SCRIPTS_DIR: &str = "lua";

/// Manages Lua scripts for Redis execution.
pub struct ScriptManager {
    scripts: RwLock<HashMap<String, Script>>,
    script_contents: RwLock<HashMap<String, String>>,
}

impl ScriptManager {
    /// Create a new script manager.
    pub fn new() -> Self {
        Self {
            scripts: RwLock::new(HashMap::new()),
            script_contents: RwLock::new(HashMap::new()),
        }
    }

    /// Load all Lua scripts from the scripts directory.
    pub async fn load_all(&self, conn: &mut Connection) -> Result<()> {
        let lua_dir = Path::new(LUA_SCRIPTS_DIR);
        if !lua_dir.exists() {
            warn!(path = %lua_dir.display(), "Lua scripts directory not found");
            return Ok(());
        }

        let entries = std::fs::read_dir(lua_dir)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().is_some_and(|ext| ext == "lua") {
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default()
                    .to_string();

                let content = std::fs::read_to_string(&path)?;
                let script = Script::new(&content);

                // Load script into Redis to get SHA
                let sha: String = redis::cmd("SCRIPT")
                    .arg("LOAD")
                    .arg(&content)
                    .query_async(conn)
                    .await?;

                info!(script_name = %name, sha = %sha[..8].to_string(), "Loaded Lua script");

                self.scripts.write().insert(name.clone(), script);
                self.script_contents.write().insert(name, content);
            }
        }

        Ok(())
    }

    /// Execute a script by name.
    ///
    /// Note: This function holds a read lock across await points. This is intentional
    /// because ScriptInvocation borrows from the Script. parking_lot's RwLock is safe
    /// for this pattern as it doesn't rely on thread-local storage.
    #[allow(clippy::await_holding_lock)]
    pub async fn execute<T: redis::FromRedisValue>(
        &self,
        conn: &mut Connection,
        script_name: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<T> {
        let scripts = self.scripts.read();
        let script = scripts
            .get(script_name)
            .ok_or_else(|| GrazeError::ScriptNotFound(script_name.to_string()))?;

        let mut invocation = script.prepare_invoke();

        for key in keys {
            invocation.key(*key);
        }
        for arg in args {
            invocation.arg(*arg);
        }

        // Try to execute with cached SHA
        match invocation.invoke_async(conn).await {
            Ok(result) => Ok(result),
            Err(e) => {
                // Check if it's a NOSCRIPT error
                let err_str = e.to_string();
                if err_str.contains("NOSCRIPT") {
                    warn!(script_name = %script_name, "Script cache miss, reloading");

                    // Reload script
                    let contents = self.script_contents.read();
                    if let Some(content) = contents.get(script_name) {
                        let _sha: String = redis::cmd("SCRIPT")
                            .arg("LOAD")
                            .arg(content)
                            .query_async(conn)
                            .await?;

                        // Retry execution
                        drop(scripts);
                        let scripts = self.scripts.read();
                        let script = scripts.get(script_name).unwrap();
                        let mut invocation = script.prepare_invoke();
                        for key in keys {
                            invocation.key(*key);
                        }
                        for arg in args {
                            invocation.arg(*arg);
                        }
                        Ok(invocation.invoke_async(conn).await?)
                    } else {
                        Err(GrazeError::ScriptNotFound(script_name.to_string()))
                    }
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Check if a script is loaded.
    pub fn has_script(&self, name: &str) -> bool {
        self.scripts.read().contains_key(name)
    }

    /// Get the names of all loaded scripts.
    pub fn script_names(&self) -> Vec<String> {
        self.scripts.read().keys().cloned().collect()
    }
}

impl Default for ScriptManager {
    fn default() -> Self {
        Self::new()
    }
}
