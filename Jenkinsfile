pipeline {
	agent {
		node {
			label 'dev'
			customWorkspace "${env.JOB_NAME}/${env.BUILD_TAG}"
		}
	}
	options {
		ansiColor('xterm')
	}
	environment {
		dockerRegistry	= "aplregistry.aarnet.edu.au"
		ImageTag        = "ci-${env.BUILD_NUMBER}"
		ImageName	= "${dockerRegistry}/cloudservices/minio/shard:${ImageTag}"
		buildImageName	= "minio-eos-build-${env.BUILD_NUMBER}"
	}
	stages {
		stage('Setup') {
			steps {
				script {
					if ( gitlabActionType == "PUSH" ) {
						currentBuild.description = "Build of '$env.gitlabBranch' started by $env.gitlabUserName"
					}
				}
				sh script: "docker build . -f Dockerfile.jenkins -t ${buildImageName}", label: "Build docker environment"
			}
		}

		stage('Lint & Formatting') {
			steps {
				catchError(buildResult: 'UNSTABLE', stageResult: 'FAILURE') {
					sh script: "docker run -t --rm ${buildImageName} make verifiers", label: "Run style verifiers"
				}
			}
		}

		stage('Build minio') {
			steps {
				sh script: "docker run -t --rm ${buildImageName} make build", label: "Build minio binary"
			}
		}

		stage ('Build and Test docker image') {
			steps {
				withDockerRegistry(url: 'https://aplregistry.aarnet.edu.au', credentialsId: 'jenkins-cloudservices-docker') {
					sh script: "docker build -t ${ImageName} -f Dockerfile.aarnet .", label: "Build cloudstor-s3-gateway docker image"
					sh script: "( cd aarnet/devenv && BUILD_TAG=${ImageTag} ./devenv -a -j )", label: "Start EOS and minio"
				}
				catchError(buildResult: 'UNSTABLE', stageResult: 'UNSTABLE') {
					sh script: "docker run --network devenv_minioshard -e SERVER_ENDPOINT=minio:9000 -e ACCESS_KEY=minioadmin -e SECRET_KEY=minioadmin -e ENABLE_HTTPS=0 minio/mint", label: "Running Mint Tests"
				}
			}
			post {
				success {
					withDockerRegistry(url: 'https://aplregistry.aarnet.edu.au', credentialsId: 'jenkins-cloudservices-docker') {
						sh script: "docker push ${ImageName}", label: "Push image to registry"
					}
				}
				unstable {
					withDockerRegistry(url: 'https://aplregistry.aarnet.edu.au', credentialsId: 'jenkins-cloudservices-docker') {
						sh script: "docker push ${ImageName}", label: "Push image to registry"
					}
				}
				always {
					sh script: "( cd aarnet/devenv && BUILD_TAG=${ImageTag} ./devenv -d -j )", label: "Tear down EOS and minio"
					sh script: "docker rmi ${ImageName}", label: "Cleanup cloudstor-s3-gateway docker image"
				}
			}
		}
	}
	post {
		always {
			sh script: "docker rmi ${buildImageName}", label: "Cleanup docker environment image"
		}
		success {
			slackSend(
				channel: 'apl-cs-pipeline',
				notifyCommitters: true,
				tokenCredentialId: 'slack-notifications-apl-cs-pipeline',
				baseUrl: 'https://aarnet.slack.com/services/hooks/jenkins-ci/',
				color: 'good',
				message: "${currentBuild.projectName} #${currentBuild.number} completed successfully.\n" + "${currentBuild.description}\n" + "${env.BUILD_URL}\n"
			)
		}
		unstable {
			slackSend(
				channel: 'apl-cs-pipeline',
				notifyCommitters: true,
				tokenCredentialId: 'slack-notifications-apl-cs-pipeline',
				baseUrl: 'https://aarnet.slack.com/services/hooks/jenkins-ci/',
				color: 'warning',
				message: "${currentBuild.projectName} #${currentBuild.number} built successfully but marked as unstable.\n" + "${currentBuild.description}\n" + "${env.BUILD_URL}\n"
			)
		}
		failure {
			slackSend(
				channel: 'apl-cs-pipeline',
				notifyCommitters: true,
				tokenCredentialId: 'slack-notifications-apl-cs-pipeline',
				baseUrl: 'https://aarnet.slack.com/services/hooks/jenkins-ci/',
				color: 'danger',
				message: "${currentBuild.projectName} #${currentBuild.number} failed.\n" + "${currentBuild.description}\n" + "${env.BUILD_URL}\n"
			)
		}
	}
};
