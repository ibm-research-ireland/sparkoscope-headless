# sparkoscope-headless
Standalone Modules for Sparkoscope

## Installation

Give mvn clean install in the root of the project
Specify the jars in spark-defaults.conf as follows:

spark.executor.extraClassPath /path/to/sparkoscope-sigarsource-1.6.2.jar:/path/to/sparkoscope-hdfssink-1.6.2.jar

Configure the metrics.properties and spark-env.sh as described in the sparkoscope project