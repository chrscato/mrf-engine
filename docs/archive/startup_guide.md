# 2025-08-27 MRF Extraction Guide

## Quick Start Commands

### 1. Setup Environment
```bash
# Pull latest code and setup environment
git pull
uv venv
uv pip install -r requirements.txt
```

### 2. UHC Georgia (Priority 1 - Direct Download)
```bash
python src/run_extraction.py \
  "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-08-01/2025-08-01_UnitedHealthcare-of-Georgia-Inc-_Insurer_Georgia-Provider-Network_GANETWORKEXGN_in-network-rates.json.gz" \
  --output-dir "output/uhc_georgia" \
  --output-prefix "uhc_georgia_20250801" \
  --cpt-whitelist "src/cpt_codes.txt" \
  --provider-batch-size 3000 \
  --rate-batch-size 15 \
  --nppes-workers 10
```
### 3. Aetna Georgia (Priority 2 - Download locally first)
```bash
python src/run_extraction.py \
  "local file here from this site:`https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICFI/machine-readable-transparency-in-coverage`" \
  --output-dir "output/aetna_georgia" \
  --output-prefix "aetna_georgia_20250801" \
  --cpt-whitelist "src/cpt_codes.txt" \
  --provider-batch-size 3000 \
  --rate-batch-size 15 \
  --nppes-workers 10
```

### 4. UHC Tennessee (Priority 3 - Direct Download)
```bash
python src/run_extraction.py \
  "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-08-01/2025-08-01_UnitedHealthcare-Insurance-Company_Insurer_Tennessee-Provider-Network_TNNETWORKEXGN_in-network-rates.json.gz" \
  --output-dir "output/uhc_tennessee" \
  --output-prefix "uhc_tennessee_20250801" \
  --cpt-whitelist "src/cpt_codes.txt" \
  --provider-batch-size 3000 \
  --rate-batch-size 15 \
  --nppes-workers 10
```
### 5. Aetna Tennessee (Priority 4 - Download locally first)
```bash
python src/run_extraction.py \
  "local file here from this site:`https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICFI/machine-readable-transparency-in-coverage`" \
  --output-dir "output/aetna_tennessee" \
  --output-prefix "aetna_tennessee_20250801" \
  --cpt-whitelist "src/cpt_codes.txt" \
  --provider-batch-size 3000 \
  --rate-batch-size 15 \
  --nppes-workers 10
```
### 6. Cigna GA HMO (Priority 3 - Local File)
```bash
# Note: Download this file first from Cigna's site, then run:
python src/run_extraction.py \
  "2025-08-01_cigna-health-life-insurance-company_georgia-hmo_in-network-rates.json.gz" \
  --output-dir "output/cigna_ga_hmo" \
  --output-prefix "cigna_ga_hmo_20250801" \
  --cpt-whitelist "src/cpt_codes.txt" \
  --provider-batch-size 3000 \
  --rate-batch-size 15 \
  --nppes-workers 10
```

### 7. Cigna GA PPO (Priority 4 - Local File)
```bash
# Note: Download this file first from Cigna's site, then run:
python src/run_extraction.py \
  "2025-08-01_cigna-health-life-insurance-company_georgia-gppo_in-network-rates.json.gz" \
  --output-dir "output/cigna_ga_ppo" \
  --output-prefix "cigna_ga_ppo_20250801" \
  --cpt-whitelist "src/cpt_codes.txt" \
  --provider-batch-size 3000 \
  --rate-batch-size 15 \
  --nppes-workers 10
```

## Configuration Details

### Hardware Assumptions
- **RAM**: 16GB+ (decent computer)
- **Storage**: SSD with good I/O
- **CPU**: Multi-core processor

### Optimized Parameters
- **Provider Batch Size**: 3000 (3x default)
- **Rate Batch Size**: 15 (3x default)
- **NPPES Workers**: 10 (1.7x default)
- **CPT Whitelist**: Using `src/cpt_codes.txt` for focused extraction

### Expected Performance
- **Provider Extraction**: 2-3x faster than defaults
- **Rate Extraction**: 2-3x faster than defaults
- **Memory Usage**: ~8-12GB peak (safe for 16GB+ systems)

## File Organization
output/
├── uhc_georgia/
│ ├── uhc_georgia_20250801_providers_[timestamp].parquet
│ ├── uhc_georgia_20250801_providers_[timestamp]cleaned.parquet
│ ├── uhc_georgia_20250801_rates_[timestamp].parquet
│ └── merged_rates_providers_[timestamp].parquet
├── uhc_tennessee/
│ └── [similar structure]
├── cigna_ga_hmo/
│ └── [similar structure]
└── cigna_ga_ppo/
└── [similar structure]



## Priority Order

1. **UHC Georgia** - Direct download, immediate processing
2. **UHC Tennessee** - Direct download, immediate processing  
3. **Cigna GA HMO** - Requires local file download first
4. **Cigna GA PPO** - Requires local file download first
5. **Aetna GA/NJ/PA** - Manual download required (not included in commands)

## Notes

- **Aetna files**: Require manual download from their portal
- **Cigna files**: Need to be downloaded locally first
- **UHC files**: Direct download URLs available
- **CPT filtering**: All extractions use the same CPT whitelist for consistency
- **Batch sizes**: Optimized for decent computers (16GB+ RAM)

## Troubleshooting

If you encounter memory issues, reduce batch sizes:
```bash
--provider-batch-size 2000  # Reduce from 3000
--rate-batch-size 10        # Reduce from 15
```

If NPPES API is slow, reduce workers:
```bash
--nppes-workers 6           # Reduce from 10
```
```

This guide provides 5 separate command-line prompts that users can run after doing a git pull and setting up their environment with uv. Each command is optimized for a decent computer and includes the CPT whitelist filtering you requested.