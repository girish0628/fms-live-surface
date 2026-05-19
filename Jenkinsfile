// All mine sites run in parallel for hourly processing.
// For full sites, during testing override the SITES parameter when triggering manually.

pipeline {
    agent any

    parameters {
        string(
            name: 'FMS_FORCE_DATE',
            defaultValue: '',
            description: 'Optional: Force a specific processing date (YYYYMMDD). Leave blank to use today. Used for manual reruns of a past date.'
        )

        string(
            name: 'FMS_RUN_TIMESTAMP',
            defaultValue: '',
            description: 'Optional: Override the run timestamp. For hourly mode use YYYYMMDDHH0000; for daily-merge use YYYYMMDD. Leave blank to auto-generate from current time.'
        )

        choice(
            name: 'ARCHIVE_DESTINATION',
            choices: ['network', 'blob', 'both'],
            description: 'Destination for nightly snippet archive: network share only, Azure Blob only, or both.'
        )
    }

    options {
        skipDefaultCheckout(true)
        disableConcurrentBuilds()
    }

    environment {
        ENV_NAME = 'NPE'

        ENABLE_WORKSPACE_CLEAN   = 'true'
        ENABLE_ARCHIVE_ARTIFACTS = 'false'

        AUTOMATION_REPO_URL = 'https://gitlab.com/bhp-cloudfactory/waio-geomatics/waio-spatial-projects/automations/mtd_fms_minestar.git'
        CONFIG_REPO_URL     = 'https://gitlab.com/bhp-cloudfactory/waio-geomatics/waio-spatial-projects/automations-config/mtd_fms_minestar_config.git'

        AUTOMATION_BRANCH = 'NPE'
        CONFIG_BRANCH     = 'NPE'
        CONFIG_SPARSE_PATH = 'NPE'

        AUTOMATION_DIR  = "${WORKSPACE}\\mtd_fms_minestar"
        CONFIG_DIR      = "${WORKSPACE}\\mtd_fms_minestar\\config"
        CONFIG_TEMP_DIR = "${WORKSPACE}\\config-repo-temp"

        SITES = 'WB,ER,SF,YND,JB,NWW,MAC'

        SKIP_MONITORING = 'true'
        DRY_RUN = 'false'
    }

    stages {
        stage('Load Config Flags') {
            steps {
                script {
                    env.ENABLE_HOURLY          = 'true'
                    env.ENABLE_HOURLY_FINALIZE = 'true'
                    env.ENABLE_DAILY_MERGE     = 'false'
                    env.ENABLE_DAILY_CLEANUP   = 'false'
                    env.ENABLE_ARCHIVE         = 'false'
                    env.ENABLE_WEEKLY          = 'false'

                    echo "Config Flags Loaded:"
                    echo "HOURLY = ${env.ENABLE_HOURLY}"
                }
            }
        }

        stage('Prepare Workspace') {
            when {
                expression {
                    return env.ENABLE_WORKSPACE_CLEAN == 'true'
                }
            }
            steps {
                echo 'Cleaning workspace before cloning...'

                cleanWs(
                    deleteDirs: true,
                    disableDeferredWipeout: true,
                    notFailBuild: true
                )
            }
        }

        stage('Checkout Repositories') {
            steps {
                echo 'Checking out automation repository...'

                dir('mtd_fms_minestar') {
                    checkout([
                        $class: 'GitSCM',
                        branches: [[name: "*/${env.AUTOMATION_BRANCH}"]],
                        userRemoteConfigs: [[
                            url: env.AUTOMATION_REPO_URL,
                            credentialsId: 'jenkins-waio-id'
                        ]],
                        extensions: [
                            [$class: 'CleanBeforeCheckout'],
                            [$class: 'CloneOption',
                                noTags: true,
                                shallow: true,
                                depth: 1,
                                timeout: 20
                            ],
                            [$class: 'CheckoutOption',
                                timeout: 20
                            ]
                        ]
                    ])
                }

                echo 'Checking out config repository into temporary folder...'

                dir('config-repo-temp') {
                    checkout([
                        $class: 'GitSCM',
                        branches: [[name: "*/${env.CONFIG_BRANCH}"]],
                        userRemoteConfigs: [[
                            url: env.CONFIG_REPO_URL,
                            credentialsId: 'jenkins-waio-id'
                        ]],
                        extensions: [
                            [$class: 'CleanBeforeCheckout'],
                            [$class: 'CloneOption',
                                noTags: true,
                                shallow: true,
                                depth: 1,
                                timeout: 20
                            ],
                            [$class: 'CheckoutOption',
                                timeout: 20
                            ],
                            [$class: 'SparseCheckoutPaths',
                                sparseCheckoutPaths: [
                                    [$class: 'SparseCheckoutPath', path: env.CONFIG_SPARSE_PATH]
                                ]
                            ]
                        ]
                    ])
                }

                bat """
                    @echo off
                    setlocal

                    echo ==========================================
                    echo Preparing flattened config folder
                    echo ENV_NAME        = %ENV_NAME%
                    echo AUTOMATION_DIR  = %AUTOMATION_DIR%
                    echo CONFIG_TEMP_DIR = %CONFIG_TEMP_DIR%
                    echo CONFIG_DIR      = %CONFIG_DIR%
                    echo ==========================================

                    if not exist "%AUTOMATION_DIR%" (
                        echo [ERROR] Automation directory not found
                        exit /b 1
                    )

                    if exist "%CONFIG_DIR%" (
                        echo Removing existing final config folder...
                        rmdir /s /q "%CONFIG_DIR%"
                    )

                    mkdir "%CONFIG_DIR%"

                    if not exist "%CONFIG_TEMP_DIR%\\%CONFIG_SPARSE_PATH%" (
                        echo [ERROR] Sparse config folder not found: %CONFIG_TEMP_DIR%\\%CONFIG_SPARSE_PATH%
                        exit /b 1
                    )

                    echo Copying config files from temp repo to final config folder...
                    xcopy "%CONFIG_TEMP_DIR%\\%CONFIG_SPARSE_PATH%\\*" "%CONFIG_DIR%\\" /E /I /Y

                    if errorlevel 1 (
                        echo [ERROR] Failed to copy config files
                        exit /b 1
                    )

                    echo Removing temporary config repo...
                    rmdir /s /q "%CONFIG_TEMP_DIR%"

                    if not exist "%CONFIG_DIR%\\app_config.yaml" (
                        echo [ERROR] app_config.yaml not found in final config folder
                        exit /b 1
                    )

                    if not exist "%CONFIG_DIR%\\logging.yaml" (
                        echo [ERROR] logging.yaml not found in final config folder
                        exit /b 1
                    )

                    echo [SUCCESS] Final config folder prepared successfully
                    dir "%CONFIG_DIR%"
                """
            }
        }

        stage('Setup Runtime') {
            steps {
                script {
                    def perthTz = TimeZone.getTimeZone('Australia/Perth')
                    def now = new Date()

                    def RUN_MODE
                    def FMS_RUN_TIMESTAMP = params.FMS_RUN_TIMESTAMP.trim()

                    def hour = now.format('HH', perthTz) as Integer
                    def minute = now.format('mm', perthTz) as Integer
                    def dayOfWeek = now.format('u', perthTz) as Integer

                    if (hour == 0) {
                        env.RUN_MODE = 'daily-merge'
                        // FMS_RUN_TIMESTAMP for daily-merge is the DATA date (yesterday),
                        // not today — the job runs at midnight but processes the prior day's files.
                        def yesterday = new Date(now.time - 24 * 60 * 60 * 1000L)
                        env.FMS_RUN_TIMESTAMP = (FMS_RUN_TIMESTAMP == '') ? yesterday.format('yyyyMMdd', perthTz) : FMS_RUN_TIMESTAMP
                    } else if (dayOfWeek == 7 && hour == 2 && minute == 30) {
                        env.RUN_MODE = 'weekly-cleanup'
                        env.FMS_RUN_TIMESTAMP = (FMS_RUN_TIMESTAMP == '') ? now.format('yyyyMMddHHmmss', perthTz) : FMS_RUN_TIMESTAMP
                    } else if (hour == 23 && minute == 30) {
                        env.RUN_MODE = 'archive'
                        env.FMS_RUN_TIMESTAMP = (FMS_RUN_TIMESTAMP == '') ? now.format('yyyyMMddHHmmss', perthTz) : FMS_RUN_TIMESTAMP
                    } else if (hour >= 1 && hour <= 23) {
                        env.RUN_MODE = 'hourly'
                        env.FMS_RUN_TIMESTAMP = (FMS_RUN_TIMESTAMP == '') ? now.format('yyyyMMddHH', perthTz) + '0000' : FMS_RUN_TIMESTAMP
                    } else {
                        env.RUN_MODE = 'skip'
                        env.FMS_RUN_TIMESTAMP = (FMS_RUN_TIMESTAMP == '') ? now.format('yyyyMMddHHmmss', perthTz) : FMS_RUN_TIMESTAMP
                    }

                    echo "RUN MODE          : ${env.RUN_MODE}"
                    echo "ENV_NAME          : ${env.ENV_NAME}"
                    echo "FMS_RUN_TIMESTAMP : ${env.FMS_RUN_TIMESTAMP}"
                    echo "SITES             : ${env.SITES}"

                    currentBuild.displayName = "#${env.BUILD_NUMBER} ${env.RUN_MODE} ${env.ENV_NAME} ${env.FMS_RUN_TIMESTAMP}"
                }
            }
        }

        stage('Hourly -- Process Sites Parallel') {
            when {
                expression {
                    return env.RUN_MODE == 'hourly' && env.ENABLE_HOURLY == 'true'
                }
            }

            steps {
                script {
                    def siteList = env.SITES
                        .split(',')
                        .collect { it.trim() }
                        .findAll { it }

                    def branches = [:]
                    def forceArg = params.FMS_FORCE_DATE.trim()
                        ? "--FMS_ForceDate ${params.FMS_FORCE_DATE.trim()}"
                        : ""

                    for (int i = 0; i < siteList.size(); i++) {
                        def site = siteList[i]

                        branches["Site ${site}"] = {
                            catchError(buildResult: 'UNSTABLE', stageResult: 'FAILURE') {
                                bat """
                                    @echo off
                                    setlocal

                                    echo ==========================================
                                    echo Hourly FMS processing
                                    echo ENV_NAME          = %ENV_NAME%
                                    echo SITE              = ${site}
                                    echo FMS_RUN_TIMESTAMP = %FMS_RUN_TIMESTAMP%
                                    echo Date              = %DATE% Time = %TIME%
                                    echo ==========================================

                                    if not exist "%WORKSPACE%\\site-status" mkdir "%WORKSPACE%\\site-status"
                                    if not exist "%WORKSPACE%\\logs" mkdir "%WORKSPACE%\\logs"

                                    cd /d "%AUTOMATION_DIR%" || exit /b 1

                                    set FMS_RUN_TIMESTAMP=%FMS_RUN_TIMESTAMP%

                                    "%ArcPy3%" ^
                                        -m src.runners.fms_runner ^
                                        --config "%CONFIG_DIR%\\app_config.yaml" ^
                                        --logging "%CONFIG_DIR%\\logging.yaml" ^
                                        --site ${site} ^
                                        --env "%ENV_NAME%" ^
                                        --FMS_RunTimestamp "%FMS_RUN_TIMESTAMP%" ${env.SKIP_MONITORING == 'true' ? '--skip-monitoring' : ''} ${forceArg}

                                    IF ERRORLEVEL 1 (
                                        echo FAILED > "%WORKSPACE%\\site-status\\hourly-${site}.txt"
                                        echo [ERROR] Hourly Python execution failed for site ${site}
                                        exit /b 1
                                    )

                                    echo SUCCESS > "%WORKSPACE%\\site-status\\hourly-${site}.txt"
                                    echo [SUCCESS] Hourly processing completed for site ${site}
                                    exit /b 0
                                """
                            }
                        }
                    }

                    branches.failFast = false
                    parallel branches
                }
            }
        }

        stage('Hourly -- Finalize Merge Boundaries + FME INGEST') {
            when {
                expression {
                    return env.RUN_MODE == 'hourly' && env.ENABLE_HOURLY_FINALIZE == 'true'
                }
            }

            steps {
                catchError(buildResult: 'UNSTABLE', stageResult: 'FAILURE') {
                    bat """
                        @echo off
                        setlocal

                        echo ==========================================
                        echo Hourly Finalize Process
                        echo ENV_NAME          = %ENV_NAME%
                        echo FMS_RUN_TIMESTAMP = %FMS_RUN_TIMESTAMP%
                        echo Date              = %DATE% Time = %TIME%
                        echo ==========================================

                        if not exist "%WORKSPACE%\\site-status" mkdir "%WORKSPACE%\\site-status"

                        cd /d "%AUTOMATION_DIR%" || exit /b 1

                        set FMS_RUN_TIMESTAMP=%FMS_RUN_TIMESTAMP%

                        "%ArcPy3%" ^
                            -m src.runners.fms_finalize_runner ^
                            --config "%CONFIG_DIR%\\app_config.yaml" ^
                            --logging "%CONFIG_DIR%\\logging.yaml" ^
                            --run-timestamp "%FMS_RUN_TIMESTAMP%" ^
                            --env "%ENV_NAME%"

                        IF ERRORLEVEL 1 (
                            echo FAILED > "%WORKSPACE%\\site-status\\hourly-finalize.txt"
                            echo [ERROR] Hourly finalize failed
                            exit /b 1
                        )

                        echo SUCCESS > "%WORKSPACE%\\site-status\\hourly-finalize.txt"
                        echo [SUCCESS] Hourly finalize completed
                        exit /b 0
                    """
                }
            }
        }

        stage('Daily -- Process Sites Parallel') {
            when {
                expression {
                    return env.RUN_MODE == 'daily-merge' && env.ENABLE_DAILY_MERGE == 'true'
                }
            }

            steps {
                script {
                    def siteList = env.SITES
                        .split(',')
                        .collect { it.trim() }
                        .findAll { it }

                    def branches = [:]

                    for (int i = 0; i < siteList.size(); i++) {
                        def site = siteList[i]

                        branches["Site ${site}"] = {
                            catchError(buildResult: 'UNSTABLE', stageResult: 'FAILURE') {
                                bat """
                                    @echo off
                                    setlocal

                                    echo ==========================================
                                    echo Daily FMS processing
                                    echo ENV_NAME          = %ENV_NAME%
                                    echo SITE              = ${site}
                                    echo FMS_RUN_TIMESTAMP = %FMS_RUN_TIMESTAMP%
                                    echo Date              = %DATE% Time = %TIME%
                                    echo ==========================================

                                    if not exist "%WORKSPACE%\\site-status" mkdir "%WORKSPACE%\\site-status"

                                    cd /d "%AUTOMATION_DIR%" || exit /b 1

                                    "%ArcPy3%" ^
                                        -m src.runners.daily_merge_runner ^
                                        --config "%CONFIG_DIR%\\app_config.yaml" ^
                                        --logging "%CONFIG_DIR%\\logging.yaml" ^
                                        --site ${site} ^
                                        --FMS_ForceDate "%FMS_RUN_TIMESTAMP%" ^
                                        --env "%ENV_NAME%"

                                    IF ERRORLEVEL 1 (
                                        echo FAILED > "%WORKSPACE%\\site-status\\daily-${site}.txt"
                                        echo [ERROR] Daily processing failed for site ${site}
                                        exit /b 1
                                    )

                                    echo SUCCESS > "%WORKSPACE%\\site-status\\daily-${site}.txt"
                                    echo [SUCCESS] Daily processing completed for site ${site}
                                    exit /b 0
                                """
                            }
                        }
                    }

                    branches.failFast = false
                    parallel branches
                }
            }
        }

        stage('Daily -- Finalize Boundaries + FME INGEST + FME DELETE') {
            when {
                expression {
                    return env.RUN_MODE == 'daily-merge' && env.ENABLE_DAILY_MERGE == 'true'
                }
            }

            steps {
                catchError(buildResult: 'UNSTABLE', stageResult: 'FAILURE') {
                    bat """
                        @echo off
                        setlocal

                        echo ==========================================
                        echo Daily Finalize Process
                        echo ENV_NAME          = %ENV_NAME%
                        echo FMS_RUN_TIMESTAMP = %FMS_RUN_TIMESTAMP%
                        echo Date              = %DATE% Time = %TIME%
                        echo ==========================================

                        if not exist "%WORKSPACE%\\site-status" mkdir "%WORKSPACE%\\site-status"

                        cd /d "%AUTOMATION_DIR%" || exit /b 1

                        "%ArcPy3%" ^
                            -m src.runners.daily_finalize_runner ^
                            --config "%CONFIG_DIR%\\app_config.yaml" ^
                            --logging "%CONFIG_DIR%\\logging.yaml" ^
                            --run-date "%FMS_RUN_TIMESTAMP%" ^
                            --env "%ENV_NAME%"

                        IF ERRORLEVEL 1 (
                            echo FAILED > "%WORKSPACE%\\site-status\\daily-finalize.txt"
                            echo [ERROR] Daily finalize failed
                            exit /b 1
                        )

                        echo SUCCESS > "%WORKSPACE%\\site-status\\daily-finalize.txt"
                        echo [SUCCESS] Daily finalize completed
                        exit /b 0
                    """
                }
            }
        }

        stage('Daily -- Cleanup Hourly Surveys FME DELETE') {
            when {
                expression {
                    return env.RUN_MODE == 'daily-cleanup' && env.ENABLE_DAILY_CLEANUP == 'true'
                }
            }

            steps {
                catchError(buildResult: 'UNSTABLE', stageResult: 'FAILURE') {
                    bat """
                        @echo off
                        setlocal

                        echo ==========================================
                        echo Daily Cleanup Process
                        echo ENV_NAME = %ENV_NAME%
                        echo Date     = %DATE% Time = %TIME%
                        echo ==========================================

                        if not exist "%WORKSPACE%\\site-status" mkdir "%WORKSPACE%\\site-status"

                        cd /d "%AUTOMATION_DIR%" || exit /b 1

                        "%ArcPy3%" ^
                            -m src.runners.daily_cleanup_runner ^
                            --config "%CONFIG_DIR%\\app_config.yaml" ^
                            --logging "%CONFIG_DIR%\\logging.yaml" ^
                            --env "%ENV_NAME%"

                        IF ERRORLEVEL 1 (
                            echo FAILED > "%WORKSPACE%\\site-status\\daily-cleanup.txt"
                            echo [ERROR] Daily cleanup failed
                            exit /b 1
                        )

                        echo SUCCESS > "%WORKSPACE%\\site-status\\daily-cleanup.txt"
                        echo [SUCCESS] Daily cleanup completed
                        exit /b 0
                    """
                }
            }
        }

        stage('Nightly -- Archive SNP Files') {
            when {
                expression {
                    return env.RUN_MODE == 'archive' && env.ENABLE_ARCHIVE == 'true'
                }
            }

            steps {
                catchError(buildResult: 'UNSTABLE', stageResult: 'FAILURE') {
                    bat """
                        @echo off
                        setlocal

                        echo ==========================================
                        echo Nightly Archive Process
                        echo ENV_NAME = %ENV_NAME%
                        echo DRY_RUN  = %DRY_RUN%
                        echo Date     = %DATE% Time = %TIME%
                        echo ==========================================

                        if not exist "%WORKSPACE%\\site-status" mkdir "%WORKSPACE%\\site-status"

                        cd /d "%AUTOMATION_DIR%" || exit /b 1

                        "%ArcPy3%" ^
                            -m src.runners.archive_runner ^
                            --config "%CONFIG_DIR%\\app_config.yaml" ^
                            --logging "%CONFIG_DIR%\\logging.yaml" ^
                            --env "%ENV_NAME%" ^
                            --destination "${params.ARCHIVE_DESTINATION}" ^
                            ${env.DRY_RUN == 'true' ? '--dry-run' : ''}

                        IF ERRORLEVEL 1 (
                            echo FAILED > "%WORKSPACE%\\site-status\\archive.txt"
                            echo [ERROR] Archive process failed
                            exit /b 1
                        )

                        echo SUCCESS > "%WORKSPACE%\\site-status\\archive.txt"
                        echo [SUCCESS] Archive process completed
                        exit /b 0
                    """
                }
            }
        }

        stage('Weekly -- Archive Old Daily Folders To Azure Blob') {
            when {
                expression {
                    return env.RUN_MODE == 'weekly-cleanup' && env.ENABLE_WEEKLY == 'true'
                }
            }

            steps {
                catchError(buildResult: 'UNSTABLE', stageResult: 'FAILURE') {
                    bat """
                        @echo off
                        setlocal

                        echo ==========================================
                        echo Weekly Cleanup Process
                        echo ENV_NAME = %ENV_NAME%
                        echo DRY_RUN  = %DRY_RUN%
                        echo Date     = %DATE% Time = %TIME%
                        echo ==========================================

                        if not exist "%WORKSPACE%\\site-status" mkdir "%WORKSPACE%\\site-status"

                        cd /d "%AUTOMATION_DIR%" || exit /b 1

                        "%ArcPy3%" ^
                            -m src.runners.weekly_cleanup_runner ^
                            --config "%CONFIG_DIR%\\app_config.yaml" ^
                            --logging "%CONFIG_DIR%\\logging.yaml" ^
                            --env "%ENV_NAME%" ^
                            ${env.DRY_RUN == 'true' ? '--dry-run' : ''}

                        IF ERRORLEVEL 1 (
                            echo FAILED > "%WORKSPACE%\\site-status\\weekly-cleanup.txt"
                            echo [ERROR] Weekly cleanup failed
                            exit /b 1
                        )

                        echo SUCCESS > "%WORKSPACE%\\site-status\\weekly-cleanup.txt"
                        echo [SUCCESS] Weekly cleanup completed
                        exit /b 0
                    """
                }
            }
        }

        stage('Dashboard Summary') {
            steps {
                script {
                    def summaryLines = []

                    summaryLines << "FMS Live Surface Summary"
                    summaryLines << "Mode: ${env.RUN_MODE}"
                    summaryLines << "Environment: ${env.ENV_NAME}"
                    summaryLines << "Run Timestamp: ${env.FMS_RUN_TIMESTAMP}"
                    summaryLines << "Build: ${env.BUILD_NUMBER}"
                    summaryLines << "--------------------------------"

                    def statusIcon = { String statusFile, String label ->
                        def status = fileExists(statusFile)
                            ? readFile(statusFile).trim()
                            : 'NOT_RUN'
                        def icon = (status == 'SUCCESS') ? '✅'
                                 : (status == 'FAILED')  ? '❌'
                                 : '⚠️'
                        if (status == 'FAILED') currentBuild.result = 'UNSTABLE'
                        summaryLines << "${icon} ${label}: ${status}"
                    }

                    if (env.RUN_MODE == 'hourly') {
                        def siteList = env.SITES
                            .split(',')
                            .collect { it.trim() }
                            .findAll { it }

                        for (site in siteList) {
                            statusIcon("site-status/hourly-${site}.txt", "Hourly ${site}")
                        }
                        statusIcon("site-status/hourly-finalize.txt", "Hourly Finalize")

                    } else if (env.RUN_MODE == 'daily-merge') {
                        def siteList = env.SITES
                            .split(',')
                            .collect { it.trim() }
                            .findAll { it }

                        for (site in siteList) {
                            statusIcon("site-status/daily-${site}.txt", "Daily ${site}")
                        }
                        statusIcon("site-status/daily-finalize.txt", "Daily Finalize")

                    } else if (env.RUN_MODE == 'archive') {
                        statusIcon("site-status/archive.txt", "Archive (${params.ARCHIVE_DESTINATION})")
                    } else {
                        statusIcon("site-status/${env.RUN_MODE}.txt", env.RUN_MODE)
                    }

                    def summaryText = summaryLines.join('\n')
                    echo summaryText

                    // Keep build description plain text because Jenkins may not render HTML here
                    currentBuild.description = summaryLines.join(' | ')
                }
            }
        }
    }

    post {
        always {
            script {
                if (env.ENABLE_ARCHIVE_ARTIFACTS == 'true') {
                    archiveArtifacts artifacts: 'logs/*.log, site-status/*.txt', allowEmptyArchive: true
                } else {
                    echo 'Artifact archive is currently disabled.'
                }
            }
        }

        unstable {
            mail(
                to: 'girish.pathak@bhp.com',
                subject: '[JENKINS UNSTABLE] FMS Live Surface -- ${env.MODE} -- ${env.ENV_NAME}',
                body: """
FMS Live Surface pipeline completed as UNSTABLE.

Mode        : ${env.RUN_MODE}
Environment : ${env.ENV_NAME}
Timestamp   : ${env.FMS_RUN_TIMESTAMP}
Build URL   : ${env.BUILD_URL}

Check dashboard summary and logs for details.
"""
            )
        }

        failure {
            mail(
                to: 'girish.pathak@bhp.com',
                subject: '[JENKINS FAILURE] FMS Live Surface -- ${env.RUN_MODE} -- ${env.ENV_NAME}',
                body: """
FMS Live Surface pipeline failed.

Mode        : ${env.RUN_MODE}
Environment : ${env.ENV_NAME}
Timestamp   : ${env.FMS_RUN_TIMESTAMP}
Build URL   : ${env.BUILD_URL}

Check the build logs for details.
"""
            )
        }
    }
}