# RADx Harmonizer: Data Validation and Harmonization Toolkit for RADx-rad Submissions

This repository provides tools for validating and harmonizing datasets submitted to the **[RADx-rad Data Coordinating Center (DCC)](https://www.radxrad.org/)** for integration into the **[NIH RADx Data Hub](https://radxdatahub.nih.gov/)**. It includes utilities to process and convert raw submission files into standardized formats for downstream use.

The following RADx-rad datasets have been harmonized using this toolkit and are available in the [NIH RADx Data Hub](https://radxdatahub.nih.gov/studyExplorer/studies?&facets=%5B%7B%22name%22:%22dcc%22,%22facets%22:%5B%22RADx-rad%22%5D%7D,%7B%22name%22:%22has_data_files%22,%22facets%22:%5B%22Yes%22%5D%7D%5D&sort=asc&prop=title&page=1&size=50&view=table).


---

## 📂 Directory Structure for RADx-DCC Data Harmonization

RADx-rad study datasets must follow this structure before harmonization:

```
data_harmonized/
└── rad_xxx_yyy-zz/                         # Unique study directory
    └── preorigcopy/                        # Raw submitted files
        ├── rad_xxx_yyy-zz_label_DATA_preorigcopy.csv
        ├── rad_xxx_yyy-zz_label_DICT_preorigcopy.csv
        ├── rad_xxx_yyy-zz_label_META_preorigcopy.csv
        └── ...
```
Each `label` is a unique user-defined string that describes each triplet of files (data, dictionary, metadata).

---

## 🛠 Harmonization Workflow

Run the following steps for each study (`rad_xxx_yyy-zz`), fixing any reported errors along the way.

### 1. Phase 1 – Validate Submission Files
```bash
python src/phase1.py -include rad_xxx_yyy-zz
```
- Output: `work/phase1_errors.csv`
- Fix files in `preorigcopy/` and rerun if needed.

### 2. Phase 2 – Standardize and Validate Copies in `work` Directory
```bash
python src/phase2.py -include rad_xxx_yyy-zz
```
- Output: `work/phase2_errors.csv`
- Fix files in `work/` and rerun if needed.

### 3. Phase 3 – Harmonize Data
```bash
python src/phase3.py -include rad_xxx_yyy-zz
```
- Output directories:
  - `origcopy/`: Harmonized raw submission files
  - `transformcopy/`: Globally harmonized Tier 1 files (optional)
- Errors: `work/phase3_errors.csv`

### 4. Upload to NIH RADx Data Hub
Submit the `origcopy/` and, if available, `transformcopy/` directories to the NIH RADx Data Hub.

---

## ⚙️ Setup Instructions

### Prerequisites

- [Miniconda3](https://docs.conda.io/en/latest/miniconda.html)
- Git
- Java 17

```bash
# Update Conda and install prerequisites
conda update conda
# Install git if not present
conda install git -n base -c anaconda
# Install Java 17 if not present
```

---

## 📥 Download Required Files

### 1. Clone Repositories
```bash
git clone https://github.com/radxrad/metadata.git
git clone https://github.com/radxrad/radx-harmonizer
cd radx-harmonizer
```

### 2. Download Validation Tools
```bash
mkdir source

# Data Dictionary Validator
wget -P source/ https://github.com/bmir-radx/radx-data-dictionary-validator/releases/download/v1.3.4/radx-data-dictionary-validator-app-1.3.4.jar

# Metadata Validator
wget -P source/ https://github.com/bmir-radx/radx-metadata-validator/releases/download/v1.0.6/radx-metadata-validator-app-1.0.6.jar

# Metadata Compiler
wget -P source/ https://github.com/bmir-radx/radx-rad-metadata-compiler/releases/download/v1.0.3/radx-rad-metadata-compiler-1.0.3.jar
```

### 3. Download Specifications and Dictionaries
```bash
mkdir reference

# Metadata Specification
wget -P reference/ https://github.com/bmir-radx/radx-metadata-validator/releases/download/v1.0.6/RADxMetadataSpecification.json

# Global Tier1 Dictionary
wget -P reference/ https://raw.githubusercontent.com/radxrad/common-data-elements/refs/heads/main/cdes/RADx-global_tier1_dict_2025-03-19.csv

# RADx-rad Tier1 and Tier2 Dictionaries
wget -P reference/ https://raw.githubusercontent.com/radxrad/common-data-elements/refs/heads/main/cdes/RADx-rad_tier1_dict_2025-03-19.csv
wget -P reference/ https://raw.githubusercontent.com/radxrad/common-data-elements/refs/heads/main/cdes/RADx-rad_tier2_dict_2025-03-19.csv

# Legacy Dictionary
wget -P reference/ https://raw.githubusercontent.com/radxrad/common-data-elements/refs/heads/main/cdes/RADx-rad_legacy_dict_2025-03-19.csv
```

---

## 🧪 Environment Setup

Create and activate the project environment using the provided `environment.yml`.

```bash
conda env create -f environment.yml
conda activate radx-harmonizer
```

To deactivate:
```bash
conda deactivate
```

---

## 📚 Related Resources

| Resource | Description |
|----------|-------------|
| [RADx Data Dictionary Specification](https://github.com/bmir-radx/radx-data-dictionary-specification/blob/main/radx-data-dictionary-specification.md) | Specification of the RADx Data Dictionary format |
| [RADx-rad Data Dictionaries](https://github.com/radxrad/common-data-elements) | Tier 1 (RADx global) and Tier 2 (RADx-rad-specific) data elements |
| [RADx-rad Metadata](https://github.com/radxrad/metadata) | Study-specific metadata files |
| [RADx-rad Publications](https://github.com/radxrad/radx-destiller/blob/main/publications/radx_rad_related_publications_2025-06-17.csv) | List of publications related to RADx-rad objectives |

---

## 📝 Citation

Peter W. Rose, RADx-rad Harmonizer: Data Validation and Harmonization Toolkit for Data Submissions, Available online: https://github.com/radxrad/radx-harmonizer (2025)

---

## 💰 Funding

Supported by the **Office of the Director, National Institutes of Health** under:

> **RADx-Rad Discoveries & Data: Consortium Coordination Center Program Organization**  
> [Grant: 7U24LM013755](https://reporter.nih.gov/project-details/10745886)