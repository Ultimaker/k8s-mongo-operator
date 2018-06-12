#!/usr/bin/groovy

// Jenkins configuration
def defaultNode = "docker"
def imageTag = "ultimaker/k8s-mongo-operator:${env.BRANCH_NAME}.${env.BUILD_NUMBER}"

node(defaultNode)
{
    stage("Checkout")
    {
        checkout scm
    }

    // Build the Docker image for this service in order to run the tests
    stage("Build")
    {
        sh "docker build --tag ${imageTag} ."
        sh "docker rmi ${imageTag}"
    }
}
