pipeline {
    agent any

    stages {
        stage('Hello') {
            steps {
                sh 'spark-submit zipcodes.py'
            }
        }
    }
}
