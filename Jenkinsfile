// =============================================================================
// FMS Live Surface — Unified Jenkins Pipeline
// =============================================================================
//
// This single Jenkinsfile drives all four FMS automation workflows.
// Create four separate Jenkins Pipeline jobs (one per mode), each pointing
// at this file in SCM and setting the MODE parameter as its default value.
//
//  Job name                 | MODE            | Trigger
//  -------------------------|-----------------|---------------------------
//  FMS-Hourly               | hourly          | cron('H * * * *')
//  FMS-Daily-Merge          | daily-merge     | cron('30 0 * * *')
//  FMS-Daily-Cleanup        | daily-cleanup   | cron('0 1 * * *')
//  FMS-Weekly-Cleanup       | weekly          | cron('0 2 * * 0')
//
// ENVIRONMENT:
//   DEPLOY_ENV is read from Jenkins Global Properties (Manage Jenkins →
//   System → Global properties → Environment variables).
//   Set DEPLOY_ENV=NPE on the NPE controller and DEPLOY_ENV=PROD on PROD.
//   No need to pass it as a build parameter — it is injected automatically.
//
// SITES:
//   All 7 mine sites run in parallel for MODE=hourly.  To run a subset
//   during testing, override the SITES parameter when triggering manually.
// =============================================================================

pipeline {
    agent any

    parameters {
        choice(
            name: 'MODE',
            choices: ['hourly', 'daily-merge', 'daily-cleanup', 'weekly'],
            description: 'Pipeline mode to execute'
        )
        string(
            name: 'SITES',
            defaultValue: 'WB,ER,SF,YND,JB,NWW,MAC',
            description: 'Comma-separated site codes for hourly mode'
        )
        string(
            name: 'CONFIG_PATH',
            defaultValue: 'config/app_config.yaml',
            description: 'App config YAML path'
        )
        string(
            name: 'LOGGING_PATH',
            defaultValue: 'config/logging.prod.yaml',
            description: 'Logging config YAML path'
        )
        booleanParam(
            name: 'SKIP_MONITORING',
            defaultValue: false,
            description: 'Skip file delivery monitoring check (hourly only)'
        )
        booleanParam(
            name: 'DRY_RUN',
            defaultValue: false,
            description: 'Dry-run mode (weekly cleanup only — no uploads/deletes)'
        )
    }

    environment {
        // DEPLOY_ENV is set in Jenkins Global Properties — do not hardcode here.
        // Fallback to 'NPE' keeps NPE jobs safe if the variable is ever unset.
        DEPLOY_ENV = "${env.DEPLOY_ENV ?: 'NPE'}"

        // Shared run timestamp: set once in the 'Setup' stage so that all
        // parallel site stages write into the same FMS_<timestamp> folder.
        FMS_RUN_TIMESTAMP = ""

        // Python venv activation command (Windows Jenkins agent)
        ACTIVATE = "venv\\Scripts\\activate.bat"
    }

    stages {

        // ----------------------------------------------------------------
        // Bootstrap
        // ----------------------------------------------------------------

        stage('Setup') {
            steps {
                bat """
                    if not exist venv python -m venv venv
                    call ${ACTIVATE}
                    python -m pip install --upgrade pip --quiet
                    pip install -r requirements.txt --quiet
                """
                script {
                    // Generate and export the shared run timestamp.
                    // All parallel hourly site stages read FMS_RUN_TIMESTAMP
                    // from the environment so they share one output folder.
                    env.FMS_RUN_TIMESTAMP = new Date().format('yyyyMMddHHmmss')
                    echo "DEPLOY_ENV    : ${env.DEPLOY_ENV}"
                    echo "MODE          : ${params.MODE}"
                    echo "RUN_TIMESTAMP : ${env.FMS_RUN_TIMESTAMP}"
                }
            }
        }

        // ----------------------------------------------------------------
        // MODE: hourly — parallel per-site processing + finalize
        // ----------------------------------------------------------------

        stage('Hourly — Process Sites (Parallel)') {
            when { expression { params.MODE == 'hourly' } }
            steps {
                script {
                    def siteList = params.SITES.split(',').collect { it.trim() }.findAll { it }
                    def parallelStages = [:]

                    siteList.each { site ->
                        def s = site  // capture for closure
                        parallelStages["Site: ${s}"] = {
                            bat """
                                call ${ACTIVATE}
                                set FMS_RUN_TIMESTAMP=${env.FMS_RUN_TIMESTAMP}
                                python -m src.runners.fms_runner ^
                                    --config ${params.CONFIG_PATH} ^
                                    --logging ${params.LOGGING_PATH} ^
                                    --site ${s} ^
                                    --env ${env.DEPLOY_ENV} ^
                                    ${params.SKIP_MONITORING ? '--skip-monitoring' : ''}
                            """
                        }
                    }

                    // Fail the stage if ANY site fails (default Jenkins behaviour
                    // for parallel is to continue; failFast stops remaining sites)
                    parallelStages.failFast = false
                    parallel parallelStages
                }
            }
        }

        stage('Hourly — Finalize (Merge Boundaries + FME INGEST)') {
            when { expression { params.MODE == 'hourly' } }
            steps {
                bat """
                    call ${ACTIVATE}
                    set FMS_RUN_TIMESTAMP=${env.FMS_RUN_TIMESTAMP}
                    python -m src.runners.fms_finalize_runner ^
                        --config ${params.CONFIG_PATH} ^
                        --logging ${params.LOGGING_PATH} ^
                        --run-timestamp ${env.FMS_RUN_TIMESTAMP} ^
                        --env ${env.DEPLOY_ENV}
                """
            }
        }

        // ----------------------------------------------------------------
        // MODE: daily-merge — mosaic hourly TIFFs + FME INGEST (Daily)
        // ----------------------------------------------------------------

        stage('Daily — Merge Hourly TIFFs + FME INGEST') {
            when { expression { params.MODE == 'daily-merge' } }
            steps {
                bat """
                    call ${ACTIVATE}
                    python -m src.runners.daily_merge_runner ^
                        --config ${params.CONFIG_PATH} ^
                        --logging ${params.LOGGING_PATH} ^
                        --env ${env.DEPLOY_ENV}
                """
            }
        }

        // ----------------------------------------------------------------
        // MODE: daily-cleanup — remove hourly surveys via FME DELETE
        // ----------------------------------------------------------------

        stage('Daily — Cleanup Hourly Surveys (FME DELETE)') {
            when { expression { params.MODE == 'daily-cleanup' } }
            steps {
                bat """
                    call ${ACTIVATE}
                    python -m src.runners.daily_cleanup_runner ^
                        --config ${params.CONFIG_PATH} ^
                        --logging ${params.LOGGING_PATH} ^
                        --env ${env.DEPLOY_ENV}
                """
            }
        }

        // ----------------------------------------------------------------
        // MODE: weekly — archive to blob + purge local file share
        // ----------------------------------------------------------------

        stage('Weekly — Archive + Cleanup File Share') {
            when { expression { params.MODE == 'weekly' } }
            steps {
                bat """
                    call ${ACTIVATE}
                    python -m src.runners.weekly_cleanup_runner ^
                        --config ${params.CONFIG_PATH} ^
                        --logging ${params.LOGGING_PATH} ^
                        --env ${env.DEPLOY_ENV} ^
                        ${params.DRY_RUN ? '--dry-run' : ''}
                """
            }
        }

    } // end stages

    post {
        always {
            archiveArtifacts artifacts: 'logs/*.log', allowEmptyArchive: true
        }
        success {
            echo "Pipeline [${params.MODE}] completed successfully on ${env.DEPLOY_ENV}"
        }
        failure {
            mail(
                to: 'gis-alerts@waio.bhp.com',
                subject: "[JENKINS FAILURE] FMS Live Surface — ${params.MODE} — ${env.DEPLOY_ENV}",
                body: """
FMS Live Surface pipeline failed.

Mode       : ${params.MODE}
Environment: ${env.DEPLOY_ENV}
Timestamp  : ${env.FMS_RUN_TIMESTAMP}
Build URL  : ${env.BUILD_URL}

Check the build logs for details.
"""
            )
        }
    }
}
