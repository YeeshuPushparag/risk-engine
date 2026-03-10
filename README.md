# Cloud-Native Cross-Asset Risk & Portfolio Analytics Platform

An end-to-end cloud-native platform for cross-asset portfolio analytics and financial risk modeling.  
The system combines batch data pipelines, real-time streaming analytics, machine learning models, and scalable cloud infrastructure to simulate how institutional investment firms run production risk systems.

The platform processes ~3200 financial instruments across multiple asset classes and provides both daily risk analytics and real-time intraday portfolio insights.

---
# System Architecture

```
                yFinance Data Source
                        |
        ------------------------------------
        |                                  |
   Daily Batch Pipeline             Intraday Streaming
        |                                  |
     Airflow                            Kafka
        |                                  |
  Feature Engineering                 Spark Streaming
  + ML Risk Models                         |
        |                                  |
  Snowflake (Analytics)               S3 Data Lake
        |                                  |
  RDS PostgreSQL                         Redis
        |                                  |
        ----------- Django Backend --------
                        |
                  Django Channels
                        |
                    WebSockets
                        |
                   Next.js Dashboard
```
---

# Daily Batch Risk Pipeline

Daily analytics jobs compute portfolio metrics and machine learning predictions.

Pipeline flow:

yFinance  
↓  
Apache Airflow (workflow orchestration)  
↓  
Feature engineering and ML models  
↓  
Snowflake (analytics warehouse)  
↓  
RDS PostgreSQL (application queries)

Daily jobs compute:

- asset returns
- rolling volatility
- drawdowns
- factor exposures
- liquidity metrics
- machine learning risk scores

Snowflake is used for large analytical queries while PostgreSQL supports application APIs and dashboard queries.

---

# Intraday Streaming Pipeline

Real-time market updates are processed through a streaming architecture.

Pipeline flow:

yFinance Intraday Data  
↓  
Kafka Producer  
↓  
Kafka Topic  
↓  
Spark Structured Streaming  
↓  
S3 Data Lake + Redis (ElastiCache)  
↓  
Django Channels  
↓  
WebSocket  
↓  
Next.js Dashboard

Redis is used as a low-latency cache for real-time dashboard updates.

---

# Backend

Backend services are implemented using Django.

Responsibilities include:

- portfolio analytics APIs
- risk model predictions
- stress testing outputs
- real-time streaming updates

Real-time communication is implemented using Django Channels.

---

# Frontend Dashboard

The frontend dashboard is built using Next.js.

Dashboard capabilities include:

- portfolio-level analytics
- manager-level metrics
- ticker-level drilldowns
- live intraday price updates
- stress testing visualization

The dashboard receives real-time updates via WebSockets.

---

# Machine Learning Models

The platform includes approximately 20 models used for financial risk analytics.

Examples include:

- Probability of Default (PD)
- Value at Risk (VaR)
- volatility forecasting
- asset clustering using K-Means

Feature engineering uses datasets from:

- market data
- macroeconomic indicators (FRED)
- regulatory filings (13F)

The data preparation, feature engineering, and model training workflows are developed in a separate repository:

https://github.com/YeeshuPushparag/risk-modeling

This repository focuses on the production risk platform where the trained models are integrated into batch and streaming analytics pipelines.

---

# Infrastructure

Infrastructure is deployed on AWS using Kubernetes (EKS).

Core components include:

- Amazon EKS Kubernetes cluster
- PostgreSQL (AWS RDS)
- Redis (AWS ElastiCache)
- S3 data lake
- Snowflake analytics warehouse
- Application Load Balancer
- Route53 DNS

Infrastructure provisioning is automated using Terraform.

---

# CI/CD Pipeline

Deployment follows a GitOps-based CI/CD workflow.

Pipeline flow:

Git Push  
↓  
GitHub Webhook  
↓  
Jenkins Pipeline  
↓  
Jenkins Agent running on EKS  
↓  
Docker Build  
↓  
Push Image to AWS ECR  
↓  
Update Helm Chart  
↓  
ArgoCD Sync  
↓  
Deploy to Kubernetes

---

# Autoscaling CI Infrastructure

Jenkins agents run dynamically inside the Kubernetes cluster.

Scaling behavior:

Idle state → 0 agents  
Pipeline triggered → agent created  
Pipeline completed → agent removed  

This design allows efficient resource utilization and cost optimization.

---

# Monitoring

System observability is implemented using:

- Prometheus
- Grafana

These monitor:

- container resource usage
- pipeline health
- service performance
- infrastructure metrics

---

# Technology Stack

Data Engineering
- Apache Spark
- Apache Kafka
- Apache Airflow

Backend
- Django
- Django Channels
- Redis

Frontend
- Next.js

Data Storage
- PostgreSQL (RDS)
- Snowflake
- S3
- Redis

Infrastructure
- Docker
- Kubernetes (EKS)
- Terraform
- Helm

CI/CD
- Jenkins
- ArgoCD
- AWS ECR

Monitoring
- Prometheus
- Grafana