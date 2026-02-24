pipeline {
    agent any

    stages {
        stage('Hello') {
            steps {
                sh '/opt/spark/bin/spark-submit zipcodes.py'
            }
        }
        stage('Hello-2') {
            steps {
                echo 'env.pyspark'
            }
        }
    }
}
