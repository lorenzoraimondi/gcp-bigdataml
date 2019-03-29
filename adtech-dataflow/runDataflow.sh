#!/usr/bin/env bash
mvn compile exec:java -e \
-Dexec.mainClass=gcp.cm.bigdata.adtech.dataflow.CleaningPipeline \
-Dexec.args="gs://bucket-for-codemotion/adtech/test \
gs://bucket-for-codemotion/adtech/test-df-out.csv \
--project=qwiklabs-gcp-1b28c6712b534a2b \
--stagingLocation=gs://bucket-for-codemotion/dataflow/staging/ \
--tempLocation=gs://bucket-for-codemotion/dataflow/temp/ \
--runner=DataflowRunner"
