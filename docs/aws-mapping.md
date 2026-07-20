# AWS production mapping

| Local component | AWS-oriented equivalent | Production changes |
|---|---|---|
| Kafka KRaft | Amazon MSK provisioned/serverless | Multi-AZ brokers, TLS/SASL, quotas, autoscaling, private networking |
| Schema Registry | AWS Glue Schema Registry or managed Confluent | IAM auth, compatibility policy as code, subject migration plan |
| Flink | Amazon Managed Service for Apache Flink | S3 checkpoints/savepoints, IAM role, autoscaling, CloudWatch alarms |
| MinIO | Amazon S3 | Bucket policies, SSE-KMS, lifecycle rules, versioning, VPC endpoints |
| Polaris/PostgreSQL | HA Polaris on ECS/EKS + Amazon RDS PostgreSQL | Multi-AZ database, backups, TLS, secrets rotation, service autoscaling |
| Trino | Trino on EKS/ECS or a managed platform | Coordinator/worker split, autoscaling, workload groups, access control |
| Prometheus/Grafana | Amazon Managed Service for Prometheus/Grafana | IAM federation, alert routing, durable retention, SLO dashboards |
| `.env` credentials | IAM roles + Secrets Manager | No static S3 keys, short-lived credentials, rotation and audit logs |

The logical table model and REST-catalog interface remain portable. The largest code change is
credential handling: engines should request scoped, short-lived credentials or assume IAM roles,
not carry access keys. Reliability testing must also cover AZ loss, broker replacement, RDS
failover, throttling, and S3 request-rate behavior rather than only container restarts.
