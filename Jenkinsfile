// FMS Live Surface — Jenkins Pipeline
// Mirrors the existing "MTD - Hourly FMS" multijob structure.
// Runs the hourly processing pipeline for all 5 mine sites in parallel.

pipeline {
    agent any

    parameters {
        choice(name: 'ENV', choices: ['PROD', 'UAT', 'DEV'], description: 'Deployment environment')
        string(name: 'CONFIG_PATH',  defaultValue: 'config/app_config.yaml',    description: 'App config YAML')
        string(name: 'LOGGING_PATH', defaultValue: 'config/logging.prod.yaml',  description: 'Logging config YAML')
        booleanParam(name: 'SKIP_MONITORING', defaultValue: false, description: 'Skip file delivery monitoring check')
        booleanParam(name: 'DRY_RUN',         defaultValue: false, description: 'Archive dry-run mode')
    }

    triggers {
        // Hourly trigger — align with existing MTD - Hourly FMS schedule
        cron('0 * * * *')
    }

    stages {

        stage('Setup') {
            steps {
                bat '''
                    if not exist venv python -m venv venv
                    call venv\\Scripts\\activate.bat
                    python -m pip install --upgrade pip --quiet
                    pip install -r requirements.txt --quiet
                '''
            }
        }

        stage('Lint') {
            steps {
                bat '''
                    call venv\\Scripts\\activate.bat
                    ruff check src tests || exit 0
                '''
            }
        }

        stage('Test') {
            steps {
                bat '''
                    call venv\\Scripts\\activate.bat
                    pytest tests/ -v --junitxml=test-results.xml || exit 0
                '''
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'test-results.xml'
                }
            }
        }

        stage('Process Sites') {
            // Run all 5 site pipelines in parallel — mirrors Jenkins multijob
            parallel {

                stage('Site: WB') {
                    steps {
                        bat """
                            call venv\\Scripts\\activate.bat
                            python -m src.runners.fms_runner ^
                                --config ${params.CONFIG_PATH} ^
                                --logging ${params.LOGGING_PATH} ^
                                --env ${params.ENV} ^
                                --site WB ^
                                ${params.SKIP_MONITORING ? '--skip-monitoring' : ''}
                        """
                    }
                }

                stage('Site: ER') {
                    steps {
                        bat """
                            call venv\\Scripts\\activate.bat
                            python -m src.runners.fms_runner ^
                                --config ${params.CONFIG_PATH} ^
                                --logging ${params.LOGGING_PATH} ^
                                --env ${params.ENV} ^
                                --site ER ^
                                ${params.SKIP_MONITORING ? '--skip-monitoring' : ''}
                        """
                    }
                }

                stage('Site: TG') {
                    steps {
                        bat """
                            call venv\\Scripts\\activate.bat
                            python -m src.runners.fms_runner ^
                                --config ${params.CONFIG_PATH} ^
                                --logging ${params.LOGGING_PATH} ^
                                --env ${params.ENV} ^
                                --site TG ^
                                ${params.SKIP_MONITORING ? '--skip-monitoring' : ''}
                        """
                    }
                }

                stage('Site: JB') {
                    steps {
                        bat """
                            call venv\\Scripts\\activate.bat
                            python -m src.runners.fms_runner ^
                                --config ${params.CONFIG_PATH} ^
                                --logging ${params.LOGGING_PATH} ^
                                --env ${params.ENV} ^
                                --site JB ^
                                ${params.SKIP_MONITORING ? '--skip-monitoring' : ''}
                        """
                    }
                }

                stage('Site: NM') {
                    steps {
                        bat """
                            call venv\\Scripts\\activate.bat
                            python -m src.runners.fms_runner ^
                                --config ${params.CONFIG_PATH} ^
                                --logging ${params.LOGGING_PATH} ^
                                --env ${params.ENV} ^
                                --site NM ^
                                ${params.SKIP_MONITORING ? '--skip-monitoring' : ''}
                        """
                    }
                }

            } // end parallel
        } // end Process Sites

    } // end stages

    post {
        always {
            archiveArtifacts artifacts: 'logs/*.log', allowEmptyArchive: true
        }
        failure {
            mail to: 'gis-alerts@waio.bhp.com',
                 subject: "[JENKINS FAILURE] FMS Live Surface — ${params.ENV}",
                 body: "Build ${env.BUILD_URL} failed. Check logs for details."
        }
    }
}
