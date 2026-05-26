# rd-giab-mock

Mock of the [kmhzamir/rd-giab](https://github.com/kmhzamir/rd-giab) rare-disease pipeline. Produces a representative tree of canned output files in **~20–40 seconds**, intended as a drop-in replacement for E2E testing of the [flowgate](https://github.com/synapsys/flowgate) orchestrator. The real pipeline takes 4+ hours per run; this exists so flowgate development isn't blocked on pipeline runtime or on transient pipeline-side failures.

**Not for scientific use.** All outputs are static fixtures copied from a single past real run — they do not reflect the input samples.

## Invocation contract

Identical to rd-giab — flowgate launches it with:

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

Accepts (but ignores) every parameter in flowgate's `pipelines/rd-giab/onprem-params.json` so the existing params file validates cleanly. See `nextflow_schema.json`.

## Local run

```bash
nextflow run main.nf -profile test,docker
```

Completes in <60s (first run ~60s for alpine pull, ~20s thereafter). Outputs land under `results/`.

To run against your own samplesheet:

```bash
nextflow run main.nf -profile docker \
  --input  path/to/samplesheet.csv \
  --outdir results
```

## Output tree

For each sample/case in the samplesheet, files land at:

```
<outdir>/
├── <case_id>/
│   ├── mitochondria/
│   │   ├── <case_id>_mito_genome.tab.gz(.tbi)
│   │   ├── <sample>_mito_merged.vcf.gz
│   │   └── <sample>_haplogroups.tsv
│   ├── snv/
│   │   ├── <sample>_snv.vcf.gz(.tbi)
│   │   ├── <sample>_rohann_vcfanno.vcf
│   │   ├── <sample>_snv_scored.vcf.gz
│   │   └── <sample>_snv_ranked.vcf.gz
│   ├── sv/
│   │   ├── <sample>_sv.vcf.gz
│   │   └── <sample>_sv_ranked.vcf.gz
│   ├── repeat_expansions/
│   │   └── <sample>_str.vcf.gz
│   └── peddy/
│       ├── <case_id>.peddy.ped
│       ├── <case_id>.sex_check.csv
│       └── <case_id>.het_check.csv
├── multiqc/
│   ├── multiqc_report.html
│   └── multiqc_data/multiqc_general_stats.txt
└── pipeline_info/
    ├── execution_report.html
    ├── execution_trace.txt
    └── timeline.html
```

## Swapping into flowgate

In flowgate's `pipelines.json`, change `rd-giab`'s `github_url` to this repo (or add a parallel entry):

```json
{
  "id": "rd-giab-mock",
  "name": "Rare Disease (mock)",
  "description": "Fast mock for E2E testing",
  "github_url": "https://github.com/gydox/rd-giab-mock",
  "revision": "main"
}
```

Restart flowgate, submit a job, expect exit-0 completion in well under a minute.

## Registering output globs in flowgate

`examples/output_curation.json` contains a ready-to-POST `OutputDecl[]` matching the tree above. After a successful run, register them via flowgate's curation endpoint to exercise the end-to-end DuckDB conversion path.

## Adjusting

- **Add another stage:** copy any `MOCK_STAGE_*` process in `workflows/raredisease.nf`, add a `publishDir` entry in `conf/modules.config`, add an `OutputDecl` to `examples/output_curation.json`. ~10 lines per stage.
- **Slow the mock down for timing tests:** `--mock_sleep_seconds 5` inserts a 5-second sleep into every stage.
- **Different canned data:** replace files in `assets/mock_outputs/`; keep the same filenames or update the `cp` lines in the workflow.

## Adding more sample data

Bundled canned files live in `assets/mock_outputs/` (~14 MB total). All sourced from a real rd-giab run.
