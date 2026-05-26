/*
 * Mock rd-giab workflow. Each MOCK_STAGE_* process copies a canned file from
 * assets/mock_outputs/ to the workdir, renaming per sample/case. publishDir
 * directives (in conf/modules.config) place the published file at the path
 * flowgate's analyst-authored OutputDecls glob against.
 */

include { fromSamplesheet } from 'plugin/nf-validation'

def maybeSleep() {
    return params.mock_sleep_seconds > 0 ? "sleep ${params.mock_sleep_seconds}" : "true"
}

process MOCK_STAGE_MITO {
    tag "${meta.case_id}/${meta.id}"
    input:
        tuple val(meta), path(canned, stageAs: 'canned/*')
    output:
        tuple val(meta), path("${meta.case_id}_mito_genome.tab.gz"),     emit: tab
        tuple val(meta), path("${meta.case_id}_mito_genome.tab.gz.tbi"), emit: tbi
        tuple val(meta), path("${meta.id}_mito_merged.vcf.gz"),          emit: vcf
    script:
        """
        ${maybeSleep()}
        cp canned/mito_genome.tab.gz     ${meta.case_id}_mito_genome.tab.gz
        cp canned/mito_genome.tab.gz.tbi ${meta.case_id}_mito_genome.tab.gz.tbi
        cp canned/mito_merged.vcf.gz     ${meta.id}_mito_merged.vcf.gz
        """
}

process MOCK_STAGE_HAPLOGREP {
    tag "${meta.case_id}/${meta.id}"
    input:
        tuple val(meta), path(canned, stageAs: 'canned/*')
    output:
        tuple val(meta), path("${meta.id}_haplogroups.tsv"), emit: tsv
    script:
        """
        ${maybeSleep()}
        cp canned/haplogroups.tsv ${meta.id}_haplogroups.tsv
        """
}

process MOCK_STAGE_SNV {
    tag "${meta.case_id}/${meta.id}"
    input:
        tuple val(meta), path(canned, stageAs: 'canned/*')
    output:
        tuple val(meta), path("${meta.id}_snv.vcf.gz"),     emit: vcf
        tuple val(meta), path("${meta.id}_snv.vcf.gz.tbi"), emit: tbi
    script:
        """
        ${maybeSleep()}
        cp canned/snv.vcf.gz     ${meta.id}_snv.vcf.gz
        cp canned/snv.vcf.gz.tbi ${meta.id}_snv.vcf.gz.tbi
        """
}

process MOCK_STAGE_SNV_ROHANN {
    tag "${meta.case_id}/${meta.id}"
    input:
        tuple val(meta), path(canned, stageAs: 'canned/*')
    output:
        tuple val(meta), path("${meta.id}_rohann_vcfanno.vcf"), emit: vcf
    script:
        """
        ${maybeSleep()}
        cp canned/rohann_vcfanno.vcf ${meta.id}_rohann_vcfanno.vcf
        """
}

process MOCK_STAGE_SNV_SCORED {
    tag "${meta.case_id}/${meta.id}"
    input:
        tuple val(meta), path(canned, stageAs: 'canned/*')
    output:
        tuple val(meta), path("${meta.id}_snv_scored.vcf.gz"), emit: vcf
    script:
        """
        ${maybeSleep()}
        cp canned/snv.vcf.gz ${meta.id}_snv_scored.vcf.gz
        """
}

process MOCK_STAGE_SNV_RANKED {
    tag "${meta.case_id}/${meta.id}"
    input:
        tuple val(meta), path(canned, stageAs: 'canned/*')
    output:
        tuple val(meta), path("${meta.id}_snv_ranked.vcf.gz"), emit: vcf
    script:
        """
        ${maybeSleep()}
        cp canned/snv.vcf.gz ${meta.id}_snv_ranked.vcf.gz
        """
}

process MOCK_STAGE_SV {
    tag "${meta.case_id}/${meta.id}"
    input:
        tuple val(meta), path(canned, stageAs: 'canned/*')
    output:
        tuple val(meta), path("${meta.id}_sv.vcf.gz"),        emit: vcf
        tuple val(meta), path("${meta.id}_sv_ranked.vcf.gz"), emit: ranked
    script:
        """
        ${maybeSleep()}
        cp canned/snv.vcf.gz ${meta.id}_sv.vcf.gz
        cp canned/snv.vcf.gz ${meta.id}_sv_ranked.vcf.gz
        """
}

process MOCK_STAGE_STR {
    tag "${meta.case_id}/${meta.id}"
    input:
        tuple val(meta), path(canned, stageAs: 'canned/*')
    output:
        tuple val(meta), path("${meta.id}_str.vcf.gz"), emit: vcf
    script:
        """
        ${maybeSleep()}
        cp canned/snv.vcf.gz ${meta.id}_str.vcf.gz
        """
}

process MOCK_STAGE_PEDDY {
    tag "${case_id}"
    input:
        tuple val(case_id), path(canned, stageAs: 'canned/*')
    output:
        tuple val(case_id), path("${case_id}.peddy.ped"),     emit: ped
        tuple val(case_id), path("${case_id}.sex_check.csv"), emit: sex
        tuple val(case_id), path("${case_id}.het_check.csv"), emit: het
    script:
        """
        ${maybeSleep()}
        cp canned/peddy.peddy.ped     ${case_id}.peddy.ped
        cp canned/peddy.sex_check.csv ${case_id}.sex_check.csv
        cp canned/peddy.het_check.csv ${case_id}.het_check.csv
        """
}

process MOCK_STAGE_MULTIQC {
    tag 'multiqc'
    input:
        path canned, stageAs: 'canned/*'
    output:
        path 'multiqc_report.html',                 emit: report
        path 'multiqc_data/multiqc_general_stats.txt', emit: stats
    script:
        """
        ${maybeSleep()}
        cp canned/multiqc_report.html multiqc_report.html
        mkdir -p multiqc_data
        cp canned/multiqc_general_stats.txt multiqc_data/multiqc_general_stats.txt
        """
}

workflow RAREDISEASE {
    if ( params.input == null ) {
        error "ERROR: --input is required (path to samplesheet CSV)"
    }
    if ( params.outdir == null ) {
        error "ERROR: --outdir is required"
    }

    ch_canned = Channel.value(file("${projectDir}/assets/mock_outputs", checkIfExists: true))

    // fromSamplesheet yields [meta_map, non_meta_field_1, non_meta_field_2, ...]
    // Our schema's non-meta columns are fastq_1 and fastq_2 (which the mock ignores).
    // All identifying fields (sample/id, case_id, sex, phenotype, paternal, maternal, lane) are in meta.
    ch_samples = Channel.fromSamplesheet('input')
        .map { meta, _fq1, _fq2 -> meta }
        .unique { (it.id ?: it.sample) + '|' + it.case_id }

    ch_cases = ch_samples
        .map { meta -> meta.case_id }
        .unique()

    ch_sample_canned = ch_samples.combine(ch_canned)
    ch_case_canned   = ch_cases.combine(ch_canned)

    MOCK_STAGE_MITO        ( ch_sample_canned )
    MOCK_STAGE_HAPLOGREP   ( ch_sample_canned )
    MOCK_STAGE_SNV         ( ch_sample_canned )
    MOCK_STAGE_SNV_ROHANN  ( ch_sample_canned )
    MOCK_STAGE_SNV_SCORED  ( ch_sample_canned )
    MOCK_STAGE_SNV_RANKED  ( ch_sample_canned )
    MOCK_STAGE_SV          ( ch_sample_canned )
    MOCK_STAGE_STR         ( ch_sample_canned )
    MOCK_STAGE_PEDDY       ( ch_case_canned )
    MOCK_STAGE_MULTIQC     ( ch_canned )
}
