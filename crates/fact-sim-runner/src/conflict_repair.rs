use std::path::Path;

use anyhow::Result;

pub use crate::sync_scale::{
    CONFLICT_REPAIR_PROFILE, GenerateOptions, LEGACY_PROFILE,
    SyncScaleReport as ConflictRepairReport, is_conflict_repair_profile,
};

pub fn generate_conflict_repair(options: GenerateOptions) -> Result<ConflictRepairReport> {
    crate::sync_scale::generate_sync_scale(options)
}

pub fn verify_conflict_repair_fixture(fixture: &Path) -> Result<ConflictRepairReport> {
    crate::sync_scale::verify_sync_scale_fixture(fixture)
}
