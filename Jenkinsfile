pipeline {
    agent any

    stages {
        stage('Hello') {
            steps {
                sh '/opt/spark/bin/spark-submit zipcodes.py'
            }
        }
    }
}
