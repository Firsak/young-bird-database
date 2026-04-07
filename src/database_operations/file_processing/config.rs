// DatabaseConfig: persistent database-wide settings
//
// Stored as a text file ({base_path}/database.conf) with key = value pairs.
// Loaded on startup, updated by SET SQL commands, persisted immediately.
//
// These are *defaults for new tables* and *runtime tunables* —
// they do NOT retroactively change existing tables (which store their own
// page_kbytes in .meta).

use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::Path,
};

use crate::database_operations::file_processing::buffer_pool::buffer_pool::DEFAULT_CACHE_SIZE;

use super::errors::DatabaseError;

/// Database-wide configuration settings.
///
/// These values serve two purposes:
/// - **Creation defaults**: `page_kbytes`, `pages_per_file`, `overflow_kbytes`
///   are used when creating new tables (existing tables keep their `.meta` values).
/// - **Runtime tunables**: `cache_size` affects buffer pool behavior for all tables.
#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseConfig {
    pub cache_size: usize,
    pub pages_per_file: u32,
    pub page_kbytes: u32,
    pub overflow_kbytes: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            cache_size: DEFAULT_CACHE_SIZE,
            pages_per_file: 1000,
            page_kbytes: 8,
            overflow_kbytes: 1024,
        }
    }
}

impl DatabaseConfig {

    pub fn get_all(&self) -> Vec<(String, String)> {
        vec![
            ("cache_size".to_string(), self.cache_size.to_string()),
            (
                "pages_per_file".to_string(),
                self.pages_per_file.to_string(),
            ),
            ("page_kbytes".to_string(), self.page_kbytes.to_string()),
            (
                "overflow_kbytes".to_string(),
                self.overflow_kbytes.to_string(),
            ),
        ]
    }

    pub fn get(&self, param: &str) -> Result<String, DatabaseError> {
        match param.to_uppercase().as_str() {
            "CACHE_SIZE" =>  Ok(self.cache_size.to_string()),
            "PAGES_PER_FILE" =>  Ok(self.pages_per_file.to_string()),
            "PAGE_KBYTES" =>  Ok(self.page_kbytes.to_string()),
            "OVERFLOW_KBYTES" =>  Ok(self.overflow_kbytes.to_string()),
            _ =>  Err(DatabaseError::InvalidArgument("Parameter name should be one of: CACHE_SIZE, PAGES_PER_FILE, PAGE_KBYTES, OVERFLOW_KBYTES".to_string()))
        }
    }

    pub fn set(&mut self, param: &str, param_value: usize) -> Result<(), DatabaseError> {
        match param.to_uppercase().as_str() {
            "CACHE_SIZE" => {self.cache_size = param_value; Ok(())},
            "PAGES_PER_FILE" => {self.pages_per_file = param_value as u32; Ok(())},
            "PAGE_KBYTES" => {self.page_kbytes = param_value as u32; Ok(())},
            "OVERFLOW_KBYTES" => {self.overflow_kbytes = param_value as u32; Ok(())},
            _ =>  Err(DatabaseError::InvalidArgument("Parameter name should be one of: CACHE_SIZE, PAGES_PER_FILE, PAGE_KBYTES, OVERFLOW_KBYTES".to_string()))
        }
    }
}

impl DatabaseConfig {
    pub fn config_path(base_path: &str) -> String {
        format!("{}/database.conf", base_path)
    }

    pub fn write_to_file(&self, path: &str) -> Result<(), DatabaseError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let mut content = String::new();
        for (key, value) in self.get_all() {
            content.push_str(&format!("{} = {}\n", key, value));
        }

        file.write_all(content.as_bytes())?;

        Ok(())
    }

    pub fn read_from_file(path: &str) -> Result<Self, DatabaseError> {
        let mut new_config = Self::default();

        if !Path::new(path).exists() {
            return Ok(new_config);
        }

        let mut file = OpenOptions::new().read(true).open(path)?;

        let mut content = String::new();
        file.read_to_string(&mut content)?;

        for line in content.lines() {
            let split_res = line.split_once('=');
            if let Some((k, v)) = split_res {
                new_config.set(
                    k.trim(),
                    v.trim().parse::<usize>().map_err(|e| {
                        DatabaseError::InvalidArgument(format!("Invalid value: {}", e))
                    })?,
                )?;
            }
        }

        Ok(new_config)
    }
}
