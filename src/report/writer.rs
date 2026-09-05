use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::schema::ReportArtifact;

const DEFAULT_REPORT_DIR: &str = ".strobengine/reports";

/// Format a filesystem-safe filename from an ISO-8601 timestamp and target URL.
///
/// Input timestamp: `"2026-08-28T09:48:19Z"`
/// Output: `"report_20260828_094819_api_example_com.json"`
pub fn format_report_filename(timestamp: &str, target_url: &str) -> String {
    // Sanitize timestamp into YYYYMMDD_HHMMSS
    let clean_time = timestamp
        .replace([':', '-'], "")
        .replace('T', "_")
        .replace('Z', "")
        .replace('+', "_");

    // Extract host from URL via string splitting (no external crate)
    let host = target_url
        .split("://")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
        .and_then(|host| host.split(':').next())
        .unwrap_or("unknown")
        .replace('.', "_");

    format!("report_{clean_time}_{host}.json")
}

/// Write a ReportArtifact to disk atomically.
///
/// The JSON is written to a temporary file in the target directory, then
/// atomically renamed to the final filename. A `latest.json` pointer file
/// is also updated atomically.
///
/// Returns the path to the written report file.
pub fn save_report_json(
    artifact: &ReportArtifact,
    output_dir: Option<&Path>,
    no_save: bool,
) -> Result<PathBuf, io::Error> {
    if no_save {
        return Ok(PathBuf::new());
    }

    let dir = output_dir.unwrap_or_else(|| Path::new(DEFAULT_REPORT_DIR));
    fs::create_dir_all(dir)?;

    let filename =
        format_report_filename(&artifact.metadata.timestamp, &artifact.metadata.target_url);
    let target_path = dir.join(&filename);
    let tmp_path = dir.join(format!(".tmp_{filename}"));

    // Atomic write: serialize → write tmp → rename to final
    let json_bytes = serde_json::to_vec_pretty(artifact)?;
    fs::write(&tmp_path, &json_bytes)?;

    // Pre-emptively clear target for cross-platform compatibility (Windows)
    let _ = fs::remove_file(&target_path);

    fs::rename(&tmp_path, &target_path)?;

    // Update latest.json pointer (also atomic)
    update_latest_pointer(dir, &filename)?;

    Ok(target_path)
}

/// Write `latest.json` atomically pointing to the most recent report file.
fn update_latest_pointer(dir: &Path, filename: &str) -> Result<(), io::Error> {
    let latest_path = dir.join("latest.json");
    let latest_tmp = dir.join(".tmp_latest.json");

    let payload = serde_json::json!({ "latest_report": filename });
    let json_bytes = serde_json::to_vec_pretty(&payload)?;

    fs::write(&latest_tmp, &json_bytes)?;

    // Pre-emptively clear existing latest.json for cross-platform compatibility (Windows)
    let _ = fs::remove_file(&latest_path);

    fs::rename(&latest_tmp, &latest_path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::schema::*;

    fn sample_artifact() -> ReportArtifact {
        let mut error_breakdown = std::collections::HashMap::new();
        error_breakdown.insert("200".to_string(), 95);
        error_breakdown.insert("500".to_string(), 5);

        ReportArtifact {
            metadata: ReportMetadata {
                timestamp: "2026-08-28T09:48:19Z".to_string(),
                duration_secs: 30.0,
                target_url: "http://api.example.com:8080/v1".to_string(),
                cli_options: CliOptions {
                    method: "GET".to_string(),
                    concurrency: 50,
                    timeout_secs: 10,
                    chaos: false,
                    chaos_rate: 0.1,
                    body: None,
                    headers: None,
                },
                system_info: SystemInfo {
                    hostname: "test-host".to_string(),
                    platform: "linux".to_string(),
                    version: "0.5.0".to_string(),
                },
            },
            summary: ReportSummary {
                total_requests: 1000,
                successful_requests: 950,
                failed_requests: 50,
                rps: 33.33,
                bytes_transferred: 524288,
            },
            latency_percentiles: LatencyPercentiles {
                p50_us: 1500.0,
                p90_us: 3000.0,
                p95_us: 4500.0,
                p99_us: 9000.0,
                min_us: 100.0,
                max_us: 15000.0,
                mean_us: 2200.0,
            },
            error_breakdown,
            avg_connection_latency_us: None,
            quic: None,
            sse: None,
            chaos: None,
        }
    }

    #[test]
    fn test_format_report_filename() {
        let name = format_report_filename("2026-08-28T09:48:19Z", "http://api.example.com:8080/v1");
        assert_eq!(name, "report_20260828_094819_api_example_com.json");
    }

    #[test]
    fn test_format_report_filename_no_port() {
        let name = format_report_filename("2026-01-01T00:00:00Z", "https://example.com");
        assert_eq!(name, "report_20260101_000000_example_com.json");
    }

    #[test]
    fn test_format_report_filename_unknown_host() {
        let name = format_report_filename("2026-01-01T00:00:00Z", "not-a-url");
        assert_eq!(name, "report_20260101_000000_unknown.json");
    }

    #[test]
    fn test_save_report_json_creates_directory() {
        let dir = std::env::temp_dir().join("strobengine_test_dir");
        let artifact = sample_artifact();
        let path = save_report_json(&artifact, Some(&dir), false).unwrap();
        assert!(path.exists());
        assert!(dir.exists());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_save_report_json_writes_valid_json() {
        let dir = std::env::temp_dir().join("strobengine_test_json");
        let artifact = sample_artifact();
        let path = save_report_json(&artifact, Some(&dir), false).unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let parsed: ReportArtifact = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed.metadata.target_url, artifact.metadata.target_url);
        assert_eq!(parsed.summary.total_requests, 1000);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_save_report_json_atomic_no_tmp_files() {
        let dir = std::env::temp_dir().join("strobengine_test_atomic");
        let artifact = sample_artifact();
        save_report_json(&artifact, Some(&dir), false).unwrap();
        let entries: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(".tmp_"))
            .collect();
        assert!(
            entries.is_empty(),
            "tmp files should not remain after write"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_latest_json_pointer() {
        let dir = std::env::temp_dir().join("strobengine_test_latest");
        let artifact = sample_artifact();
        let path = save_report_json(&artifact, Some(&dir), false).unwrap();
        let latest = fs::read_to_string(dir.join("latest.json")).unwrap();
        let pointer: serde_json::Value = serde_json::from_str(&latest).unwrap();
        assert_eq!(
            pointer["latest_report"],
            path.file_name().unwrap().to_str().unwrap()
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_latest_json_overwrite() {
        let dir = std::env::temp_dir().join("strobengine_test_latest_overwrite");
        let artifact = sample_artifact();

        save_report_json(&artifact, Some(&dir), false).unwrap();
        let latest1: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(dir.join("latest.json")).unwrap()).unwrap();
        let filename1 = latest1["latest_report"].as_str().unwrap().to_string();

        // Second write — may produce same filename if within same second
        save_report_json(&artifact, Some(&dir), false).unwrap();
        let latest2: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(dir.join("latest.json")).unwrap()).unwrap();
        let filename2 = latest2["latest_report"].as_str().unwrap().to_string();

        // latest.json should exist and point to a valid report file
        assert!(dir.join(&filename1).exists());
        assert!(dir.join(&filename2).exists());
        // latest.json should have been updated
        assert_eq!(latest2["latest_report"], filename2);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_no_save_returns_early() {
        let artifact = sample_artifact();
        let path = save_report_json(&artifact, None, true).unwrap();
        assert!(path.as_os_str().is_empty());
    }

    #[test]
    fn test_default_output_dir() {
        let artifact = sample_artifact();
        let path = save_report_json(&artifact, None, false).unwrap();
        let path_str = path.to_string_lossy();
        assert!(
            path_str.contains(".strobengine/reports"),
            "expected path to contain .strobengine/reports, got: {path_str}"
        );
        fs::remove_dir_all("./.strobengine").ok();
    }

    #[test]
    fn test_save_report_json_overwrites_existing_target() {
        let dir = std::env::temp_dir().join("strobengine_test_overwrite");
        let artifact = sample_artifact();

        // First write — creates the file
        let path1 = save_report_json(&artifact, Some(&dir), false).unwrap();
        assert!(path1.exists());
        let content1 = fs::read_to_string(&path1).unwrap();
        assert!(content1.contains("metadata"));

        // Second write — should overwrite (simulates same-second collision)
        let path2 = save_report_json(&artifact, Some(&dir), false).unwrap();
        assert!(path2.exists());
        let content2 = fs::read_to_string(&path2).unwrap();
        assert!(content2.contains("metadata"));

        // Both paths should be the same (same filename)
        assert_eq!(path1, path2);

        fs::remove_dir_all(&dir).ok();
    }
}
