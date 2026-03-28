# startup.sh
#!/bin/bash

airflow variables set s3_bucket "yf-elt-data-dev-402705369995"

airflow variables set ecs_config '{
  "cluster_arn": "arn:aws:ecs:ap-southeast-2:402705369995:cluster/yf-elt-cluster-dev",
  "task_definition": "arn:aws:ecs:ap-southeast-2:402705369995:task-definition/yf-elt-extractor-dev:1",
  "container_name": "extractor",
  "subnets": ["subnet-0d6bfd621abb49ba6", "subnet-0b627c1fce04d0996"],
  "security_group": "sg-00c414a12234de2fe"
}'

airflow variables set sns_topic_arn "arn:aws:sns:ap-southeast-2:402705369995:yf-elt-airflow-alerts-dev"