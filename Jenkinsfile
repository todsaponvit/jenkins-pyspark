pipeline {
    agent any

    stages {
        stage('zipcodes') {
            steps {
                sh '/opt/spark/bin/spark-submit zipcodes.py'
            }
        }
        stage('rustfs') {
            steps {
                sh '/opt/spark/bin/spark-submit rustfs.py'
            }
        }
    }
}
