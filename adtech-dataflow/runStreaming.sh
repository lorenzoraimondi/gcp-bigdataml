#!/usr/bin/env bash
mvn compile exec:java -e \
-Dexec.mainClass=gcp.cm.bigdata.adtech.dataflow.StreamingPipeline \
-Dexec.args="qwiklabs-gcp-56c3e61809c73e4d \
gs://abucket-for-codemotion/dataflow/staging/ \
gs://abucket-for-codemotion/dataflow/temp/ \
ad-impressions"
