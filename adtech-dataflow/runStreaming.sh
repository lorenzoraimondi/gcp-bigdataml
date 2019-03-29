#!/usr/bin/env bash
mvn compile exec:java -e \
-Dexec.mainClass=gcp.cm.bigdata.adtech.dataflow.StreamingPipeline \
-Dexec.args="qwiklabs-gcp-1b28c6712b534a2b \
gs://bucket-for-codemotion/dataflow/staging/ \
gs://bucket-for-codemotion/dataflow/temp/ \
ad-impressions \
gs://bucket-for-codemotion/dataflow/"
