# Cloud-Native Cross-Asset Risk & Portfolio Analytics Platform

An end-to-end cloud-native platform for cross-asset portfolio analytics and financial risk modeling.  
The system combines batch data pipelines, real-time streaming analytics, machine learning models, and scalable cloud infrastructure to simulate how institutional investment firms run production risk systems.

The platform processes ~3200 financial instruments across multiple asset classes and provides both daily risk analytics and real-time intraday portfolio insights.

---
# System Architecture

<img width="1408" height="768" alt="System Architecture" src="https://github.com/user-attachments/assets/8280232d-ac96-485b-8c23-b289fcc51e9c" />

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
<img width="1366" height="768" alt="airflow" src="https://github.com/user-attachments/assets/7341c915-b3f9-4872-b4f9-fa3e1dffc8d1" />

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
<img width="1366" height="768" alt="django" src="https://github.com/user-attachments/assets/dfbc3670-59c9-4b79-bcfb-5fe76a5c4889" />

Backend services are implemented using Django.

Responsibilities include:

- portfolio analytics APIs
- stress testing outputs
- real-time streaming updates

Real-time communication is implemented using Django Channels.

---

# Frontend Dashboard
<img width="1366" height="768" alt="dashboard" src="https://github.com/user-attachments/assets/b87eaa83-857a-4b5c-944c-d25fb7aa9fdf" />

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

# Gitops Workflow
<img width="1408" height="768" alt="gitops workflow" src="https://github.com/user-attachments/assets/d78e6ca7-6364-4a6c-95c5-319a13da624e" />

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
<img width="1366" height="768" alt="ci" src="https://github.com/user-attachments/assets/a15ed150-680e-46c2-bbad-634d0571d849" />
<img width="1366" height="768" alt="cd" src="https://github.com/user-attachments/assets/45c977c8-d288-4fb3-89e0-0096f2e46d2a" />

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
<img width="1366" height="768" alt="monitoring" src="https://github.com/user-attachments/assets/d69554dd-988d-4253-adbe-c417cd002b46" />

<img width="1366" height="768" alt="prometheus" src="https://github.com/user-attachments/assets/1fbdfd7c-3cc2-4bcb-8cea-35b7a2ee0666" />

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
