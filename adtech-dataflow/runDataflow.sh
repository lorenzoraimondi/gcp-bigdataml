#!/usr/bin/env bash
mvn compile exec:java -e \
-Dexec.mainClass=gcp.cm.bigdata.adtech.dataflow.CleaningPipeline \
-Dexec.args="--project=qwiklabs-gcp-56c3e61809c73e4d \
--stagingLocation=gs://abucket-for-codemotion/dataflow/staging/ \
--tempLocation=gs://abucket-for-codemotion/dataflow/temp/ \
--runner=DataflowRunner"
