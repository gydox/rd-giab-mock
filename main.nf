#!/usr/bin/env nextflow
/*
 * gydox/rd-giab-mock
 * Mock rd-giab pipeline for flowgate E2E testing.
 * See README.md.
 */

nextflow.enable.dsl = 2

include { validateParameters; paramsSummaryLog } from 'plugin/nf-validation'
include { RAREDISEASE } from './workflows/raredisease'

workflow {
    if ( params.help ) {
        log.info "Usage: nextflow run gydox/rd-giab-mock --input samplesheet.csv --outdir results -profile docker"
        return
    }

    if ( params.validate_params ) {
        validateParameters()
    }
    log.info paramsSummaryLog(workflow)

    RAREDISEASE()
}
