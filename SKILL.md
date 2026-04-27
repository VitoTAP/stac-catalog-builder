---
name: stac-catalog-builder
description: "Canonical root skill content for onboarding and building new STAC collections in this repository."
---

# New STAC Collection Builder

Use this skill when creating a new dataset configuration under configs-datasets and you want a repeatable process from intake to validated collection output.

## Expected Outcome
- A new dataset folder derived from the repository template.
- A valid config-collection.json with parser and item_assets fully defined.
- A runnable workflow.py using load_env and .env.template.
- A validated collection build and a clear summary of assumptions and open gaps.

## Procedure
1. Run intake and gather missing facts.
2. Scaffold from template files.
3. Define input path parsing and item identity rules.
4. Derive asset types and band metadata from docs and sample files.
5. Fill and validate config-collection.json.
6. Configure workflow.py and environment template.
7. Dry-run checks, build, and validate output.
8. Summarize decisions, assumptions, and unresolved gaps.

## 1) Intake and Required Inputs
Do not start implementation until minimum inputs are available.

### Dataset Identity
- Dataset folder name under configs-datasets.
- collection_id.
- Collection title and description.
- Temporal extent and update cadence.

### Source Data and Naming
- Storage location and access method.
- At least 5 to 10 representative source file paths.
- Expected file format and extension.
- Naming convention rules and known edge cases.

### Time and Item Logic
- Period type: yearly, monthly, dekad, or custom.
- Which filename parts map to year, month, day, season.
- Item grouping rule and final item_id format.

### Bands and Asset Types
- Full list of asset_type values expected from file names.
- Official documentation links for each band meaning.
- Band units, scale, offset, nodata behavior.
- Data type and spatial resolution per band.

### STAC Metadata
- Instruments, mission, platform, keywords.
- Provider list with roles and URL.
- Any required custom item properties.

### Runtime and Delivery
- .env requirements and default values.
- Output path and versioning strategy.
- Whether upload is needed now or later.

### Validation Targets
- Minimum sample size for dry run.
- Conditions that must pass before full run.
- Expected output structure and sanity checks.

If key inputs are missing, ask concise clarifying questions before editing files.

## 2) Scaffold from Template
Start from the template and replace placeholders incrementally.

### Starting Point Files
- configs-datasets/config-template/config-collection.json
- configs-datasets/config-template/workflow.py
- configs-datasets/config-template/.env.template
- docs/how-to-configure-new-dataset.md
- docs/workflow.md

### Implemented Example
Use HRLVPP as a concrete reference:
- configs-datasets/HRLVPP/config-collection.json
- configs-datasets/HRLVPP/workflow.py

## 3) Define Parser and Identity
- Build regex_pattern from real file paths, never from assumptions.
- Extract asset_type and period fields required by period.
- Use fixed_values only for truly constant values.
- Ensure item_id generation is deterministic and stable.

Decision branch:
- If filename conventions are inconsistent, propose a custom parser class and document why.

## 4) Build item_assets from Product Documentation
Translate official product documentation into item_assets entries.

For each asset_type:
- Create one item_assets entry keyed by asset_type.
- Set title equal to the asset key.
- Add asset-level description.
- Add bands with at least name and description.
- If one band is present, mirror band description at asset level.
- Keep raster metadata aligned with actual data: data type, resolution, scale, offset, nodata semantics.

Decision branch:
- If official docs are incomplete, ask for additional links.
- If user prefers progress before full certainty, mark descriptions as provisional and call it out explicitly.

## 5) Validate Config Quality Gates
Before heavy build steps, enforce:
- JSON has no diagnostics errors.
- Every item_assets key exists in parser asset_type mapping.
- Every asset has title and description.
- Every band has name and description.
- Mandatory collection metadata fields are populated.

## 6) Configure Workflow and Environment
- Use load_env in workflow.py to load environment variables.
- Keep .env.template with required keys and defaults.
- Use script-relative paths based on file location for reproducibility.

## 7) Build and Verify
Recommended run order:
1. list_input_files
2. list_asset_metadata
3. list_stac_items
4. build_collection
5. validate_collection

For large datasets:
- First run a limited sample.
- Then run full build after sample checks pass.

## 8) Completion Checklist
A task is complete only when:
- Config fields are filled and validated.
- Parser extracts expected fields from sample paths.
- item_assets definitions are complete and sourced from authoritative links.
- Workflow runs with .env loading.
- Collection validates successfully.
- Final summary includes source links used for band descriptions.

## Common Gotchas
- Missing asset-level title or description can break validation.
- Band descriptions copied but asset descriptions omitted.
- Parser regex not matching all source files.
- Missing required .env variables.

## Suggested Command Flow
- uv sync --dev
- Run workflow sections incrementally before full publish/upload steps.

## Response Style for This Skill
- Ask targeted questions only when data is missing.
- Keep assumptions explicit and minimal.
- Show exactly what changed and how it was validated.
- Prefer precise, incremental edits over broad rewrites.
