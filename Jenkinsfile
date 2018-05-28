#!/usr/bin/groovy

// Jenkins configuration
def defaultNode = "docker"

node(defaultNode)
{
    stage("Checkout")
    {
        checkout scm
    }

    // Build the Docker image for this service in order to run the tests
    stage("Build")
    {
        sh "docker build -t ultimaker/k8s-mongo-operator:tests ."
    }
}
