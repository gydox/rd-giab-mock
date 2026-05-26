# rd-giab-mock

A **mock** of the [kmhzamir/rd-giab](https://github.com/kmhzamir/rd-giab) rare-disease pipeline. Produces a representative tree of canned output files in **~20–40 seconds** instead of the real pipeline's **4+ hours**. Built as a drop-in replacement so [flowgate](https://github.com/synapsys/flowgate) E2E development isn't blocked on pipeline runtime or transient pipeline-side failures.

> ⚠️ **Not for scientific use.** Every output file is a static fixture copied from a single past real run. The outputs do not reflect the input sample at all — same VCF, same TSV, every time. The point is shape, not content.

---

## Why this exists

The real rd-giab pipeline does heavy bioinformatics: bwa-mem2 alignment, GATK HaplotypeCaller, VEP annotation, vcfanno, CADD scoring, genmod ranking, etc. Each step needs gigabytes of reference data and an hour or more of compute. A full run is 4+ hours.

When you're iterating on **flowgate** (the orchestrator that submits jobs, launches Nextflow, captures outputs, and converts them to queryable Parquet), waiting 4 hours per round-trip is a deal-breaker. Worse, transient pipeline-side errors (a missing CADD index, a flaky container pull) get confused with actual flowgate bugs.

This mock breaks that dependency. It accepts the exact same invocation as the real pipeline, produces files at the exact same publishDir paths, and exits 0 in under a minute. Flowgate can't tell the difference.

---

## What it actually does

For each `(sample, case_id)` row in the samplesheet, the mock runs ~10 trivial Nextflow processes in parallel. Each one is just a `cp` of a bundled canned file into the workdir, with a `publishDir` directive that places the file at the conventional nf-core/raredisease output path.

```
samplesheet.csv (1 row)
        │
        ▼
fromSamplesheet → ch_samples ──┬──▶ MOCK_STAGE_MITO       ──▶ <case>/mitochondria/<case>_mito_genome.tab.gz
                                ├──▶ MOCK_STAGE_HAPLOGREP  ──▶ <case>/mitochondria/<sample>_haplogroups.tsv
                                ├──▶ MOCK_STAGE_SNV        ──▶ <case>/snv/<sample>_snv.vcf.gz
                                ├──▶ MOCK_STAGE_SNV_ROHANN ──▶ <case>/snv/<sample>_rohann_vcfanno.vcf
                                ├──▶ MOCK_STAGE_SNV_SCORED ──▶ <case>/snv/<sample>_snv_scored.vcf.gz
                                ├──▶ MOCK_STAGE_SNV_RANKED ──▶ <case>/snv/<sample>_snv_ranked.vcf.gz
                                ├──▶ MOCK_STAGE_SV         ──▶ <case>/sv/<sample>_sv[_ranked].vcf.gz
                                ├──▶ MOCK_STAGE_STR        ──▶ <case>/repeat_expansions/<sample>_str.vcf.gz
                                ├──▶ MOCK_STAGE_PEDDY      ──▶ <case>/peddy/<case>.peddy.ped + sex/het CSVs
                                └──▶ MOCK_STAGE_MULTIQC    ──▶ multiqc/multiqc_report.html
```

Inside each process is something like:

```groovy
process MOCK_STAGE_SNV {
    container 'alpine:3.19'
    input:  tuple val(meta), path(canned, stageAs: 'canned/*')
    output: tuple val(meta), path("${meta.id}_snv.vcf.gz")
    script: """
        cp canned/snv.vcf.gz ${meta.id}_snv.vcf.gz
    """
}
```

That's it. No bwa, no GATK, no VEP. Just `cp` in a 5-MB alpine container.

### Why use Nextflow at all?

Flowgate launches pipelines with `nextflow run <github_url> -r <revision>` — it clones the repo from GitHub and executes it as a real Nextflow pipeline. A shell script wouldn't be invocable the same way. So the mock has to *be* a Nextflow pipeline; it just stubs out the work inside each process.

### Why canned real outputs (not synthetic / empty files)?

The mock's outputs are processed by **flowgate's `result_processing.py`**, which runs them through DuckDB to convert into queryable Parquet. DuckDB needs valid file formats:

- VCF files need a `##fileformat=VCFv4.2` header and a `#CHROM` line with real columns
- TSV/CSV files need a real header row and at least one data row
- Empty files would parse as zero-row tables and likely fail downstream queries

So instead of synthesizing fake VCFs, the mock ships real ones from a past rd-giab run (bundled under `assets/mock_outputs/`, ~14 MB total). Bytes are identical to what flowgate would see in production.

### Why `alpine:3.19`?

Flowgate hardcodes `-profile docker` in its `nextflow run` command. Nextflow needs a container per process under that profile. Alpine is a 5-MB image, only pulled once per host. No real tools needed — `sh` and `cp` are built in.

---

## Invocation contract (identical to rd-giab)

Flowgate's `app/runner.py` builds this exact command:

```
nextflow run gydox/rd-giab-mock -r main \
  -c <flowgate-managed nextflow-onprem.config> \
  -profile docker \
  -with-weblog http://localhost:8000/webhooks/nextflow \
  --input  s3://flowgate/runs/<job_id>/inputs/samplesheet.csv \
  --outdir s3://flowgate/runs/<job_id>/outputs \
  -work-dir /home/bioinfo/nextflow-work/<job_id> \
  [-params-file /tmp/<job_id>-params.json]
```

The mock honors every part of this:

| Contract | How the mock satisfies it |
|---|---|
| `-r main` | Repo's default branch is `main` |
| `-profile docker` | `nextflow.config` declares the profile; alpine container per process |
| `-with-weblog` | Standard Nextflow flag, works automatically |
| `--input s3://...` | `Channel.fromSamplesheet` reads S3 URIs natively (given AWS creds from env) |
| `--outdir s3://...` | `publishDir` mode 'copy' uploads to S3 natively |
| `-params-file <flowgate's onprem-params.json>` | `nextflow_schema.json` declares all 60+ keys (fasta, gnomad, vep_cache, cadd, vcfanno, etc.) as **optional** and **ignored** — validation passes, the values are never read |
| Exit code 0 = success | `cp` succeeds → exit 0 |

---

## Output tree

For samplesheet with sample=`hugelymodelbat`, case_id=`justhusky`:

```
<outdir>/
├── justhusky/
│   ├── mitochondria/
│   │   ├── justhusky_mito_genome.tab.gz       (80 KB, bgzipped TSV)
│   │   ├── justhusky_mito_genome.tab.gz.tbi   (tabix index, 82 B)
│   │   ├── hugelymodelbat_mito_merged.vcf.gz  (12 MB, real VCF)
│   │   └── hugelymodelbat_haplogroups.tsv     (synthesized 2-row TSV)
│   ├── snv/
│   │   ├── hugelymodelbat_snv.vcf.gz          (1.5 MB, real VCF)
│   │   ├── hugelymodelbat_snv.vcf.gz.tbi      (tabix index)
│   │   ├── hugelymodelbat_rohann_vcfanno.vcf  (21 KB, real ROH-annotated VCF)
│   │   ├── hugelymodelbat_snv_scored.vcf.gz   (same VCF re-published)
│   │   └── hugelymodelbat_snv_ranked.vcf.gz   (same VCF re-published)
│   ├── sv/
│   │   ├── hugelymodelbat_sv.vcf.gz
│   │   └── hugelymodelbat_sv_ranked.vcf.gz
│   ├── repeat_expansions/
│   │   └── hugelymodelbat_str.vcf.gz
│   └── peddy/
│       ├── justhusky.peddy.ped                (synthesized PED file)
│       ├── justhusky.sex_check.csv
│       └── justhusky.het_check.csv
├── multiqc/
│   ├── multiqc_report.html
│   └── multiqc_data/
│       └── multiqc_general_stats.txt
└── pipeline_info/
    ├── execution_report.html                  (auto-generated by Nextflow)
    ├── execution_trace.txt                    (auto-generated by Nextflow)
    └── timeline.html                          (auto-generated by Nextflow)
```

---

## Wiring this into flowgate

### 1. Point flowgate at the mock

Edit `synapsys/flowgate/pipelines.json`:

```json
{
  "id": "rd-giab-mock",
  "name": "Rare Disease (mock)",
  "description": "Fast mock for E2E testing — fixture outputs only",
  "github_url": "https://github.com/gydox/rd-giab-mock",
  "revision": "main"
}
```

Either add this alongside the existing `rd-giab` entry (so both are selectable in the UI) or replace `rd-giab`'s `github_url` for full swap. Restart flowgate.

### 2. Register the output globs

The mock ships `examples/output_curation.json` — a ready-to-POST `OutputDecl[]` array matching every published file. Send it to flowgate's curation endpoint after registering the pipeline:

```bash
curl -X POST \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  --data @examples/output_curation.json \
  http://localhost:8000/pipelines/rd-giab-mock/curation-versions/output
```

### 3. Submit a job

Use the flowgate UI or API with any valid samplesheet (sample/case_id are the only fields the mock actually reads; fastq paths are ignored). Expected behavior:

- Pipeline clones from GitHub (~5 s)
- Nextflow JVM startup (~10 s)
- 10 alpine processes run in parallel (~10 s with cold container pull, ~5 s warm)
- Outputs land in MinIO under `runs/<job_id>/outputs/...`
- Exit 0 → flowgate marks `SUCCEEDED`
- Result processing converts every OutputDecl glob match to Parquet via DuckDB
- Queryable via flowgate's `/results` API

Total wall time: **20–40 seconds**, depending on container cache state.

---

## Local testing notes

> **I (Claude) could not run the mock locally** when building this repo — Nextflow isn't installed on the user's Windows machine, and Nextflow doesn't officially support native Windows (needs Linux/macOS or WSL2). Verification will happen via the flowgate docker stack, which has Nextflow installed.

If you want a faster iteration loop locally, three options in order of effort:

**Option A — WSL2 (recommended, ~10 min one-time setup):**

```bash
# in PowerShell (admin)
wsl --install
# then inside the WSL Ubuntu prompt:
curl -s https://get.nextflow.io | bash
sudo mv nextflow /usr/local/bin/
cd /mnt/c/Users/shawn.tay/workspace/rd-giab-mock
nextflow run main.nf -profile test,docker
```

**Option B — Nextflow Docker image (no local install):**

```bash
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v //c/Users/shawn.tay/workspace/rd-giab-mock:/pipe \
  -w /pipe \
  nextflow/nextflow:23.10.0 \
  nextflow run main.nf -profile test,docker
```

(Docker-in-Docker is finicky on Windows. WSL2 is smoother.)

**Option C — Just test via flowgate.** Skip local entirely. Push a change, flowgate's next job pulls the new revision and runs it. Slowest iteration but no local setup.

---

## Adjusting / extending

| To do this | Edit |
|---|---|
| Add a new output stage | Copy a `MOCK_STAGE_*` process in `workflows/raredisease.nf`, add a `publishDir` entry in `conf/modules.config`, add an `OutputDecl` to `examples/output_curation.json`. ~10 lines per stage. |
| Use different canned data | Drop files into `assets/mock_outputs/`, update the `cp` lines in `workflows/raredisease.nf`. Keep formats valid (real VCF/TSV) or DuckDB conversion in flowgate will fail. |
| Simulate a slow stage | `--mock_sleep_seconds 5` adds a 5-second sleep to every process. |
| Change the container | Default is `alpine:3.19` in `nextflow.config`. Replace with anything that has `sh` + `cp`. |
| Force a failure | Edit any `script:` block to `exit 1`. Useful for testing flowgate's failure handling. |

---

## Repo layout

```
.
├── main.nf                          # Entry: validate params, call RAREDISEASE workflow
├── nextflow.config                  # Profiles (docker/singularity/test), default container, params declarations
├── nextflow_schema.json             # input/outdir required; 60+ ref-data params accepted as optional
├── workflows/
│   └── raredisease.nf               # The 10 MOCK_STAGE_* processes + workflow wiring
├── conf/
│   └── modules.config               # publishDir directives for each process
├── assets/
│   ├── schema_input.json            # Samplesheet schema (fastq existence check disabled)
│   ├── samplesheet.csv              # Local test fixture (1 sample, 1 case)
│   └── mock_outputs/                # 11 canned files copied into the output tree
│       ├── mito_genome.tab.gz       # 80 KB, real bgzipped TSV from past run
│       ├── mito_genome.tab.gz.tbi
│       ├── mito_merged.vcf.gz       # 12 MB, real VCF
│       ├── snv.vcf.gz               # 1.5 MB, real VCF
│       ├── snv.vcf.gz.tbi
│       ├── rohann_vcfanno.vcf       # 21 KB, real ROH-annotated VCF
│       ├── haplogroups.tsv          # Synthesized 2-row TSV
│       ├── peddy.peddy.ped          # Synthesized
│       ├── peddy.sex_check.csv      # Synthesized
│       ├── peddy.het_check.csv      # Synthesized
│       ├── multiqc_report.html      # Tiny HTML stub
│       └── multiqc_general_stats.txt
└── examples/
    └── output_curation.json         # 14-entry OutputDecl[] for flowgate
```

Total: ~14 MB. Everything else (modules, subworkflows, bin scripts, docs, lib) was deleted from the upstream fork.
