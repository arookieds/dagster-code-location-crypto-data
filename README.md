# **Data Engineering \- Dagster Orchestration for Crypto Trading**

Date Deployed: 2025-12-14  
Version: Dagster 1.0  
Namespace: dagster

## **Table of Contents**

1. [Overview](#1-overview)  
2. [Prerequisites](#2-prerequisites)  
3. [Deployment](#3-deployment)  
4. [Kubernetes Resources](#4-kubernetes-resources)  
5. [Configuration](#5-configuration)  
6. [Networking](#6-networking)  
7. [Data Management](#7-data-management)  
8. [Troubleshooting](#8-troubleshooting)  
9. [Maintenance](#9-maintenance)

## **1\. Overview**

### **1.1 Purpose**

This repository contains the **Dagster Code Location** for the Crypto Data Pipeline. It defines the assets and jobs required to:

1. **Extract** market data from centralized exchanges (Binance, ByBit, etc.).  
2. **Load** raw JSON data into **MinIO** (Object Storage).  
3. **Transform** data into a relational format in **PostgreSQL**.

### **1.2 Architecture**

This project utilizes a **hybrid deployment strategy**:

1. **Containerization (Docker):** The Dockerfile packages the user code and dependencies.  
2. **Manifests (Helm \+ Kustomize):** We leverage the official **Dagster User Code Helm Chart** as a base. We use Kustomize to "inflate" this chart and apply local configurations (overlays) without maintaining raw deployment manifests.  
3. **Execution (K8sRunLauncher):** When a job runs, the Dagster Daemon launches a separate, ephemeral pod using the image defined in this repository.

```mermaid
flowchart LR
    Daemon[Dagster Daemon] -->|gRPC| CodeLoc[Code Location Pod]  
    Daemon -->|Launch| JobPod[Ephemeral Job Pod]  
    JobPod -->|Write| MinIO[(MinIO)]
    JobPod -->|Write| Postgres[(PostgreSQL)]
```

## **2\. Prerequisites**

### **2.1 Dependencies**

* **Dagster Instance:** A running Dagster platform (Webserver/Daemon).  
* **MinIO:** Reachable at minio.lxc.svc.cluster.local.  
* **PostgreSQL:** Reachable at postgresql.database.svc.cluster.local.  
* **Tools:** kubectl, kustomize, docker, sops (optional but recommended).

### **2.2 Required Secrets**

Secrets are managed via SealedSecrets.

* dagster-crypto-secrets: Contains POSTGRESQL\_PASSWORD and MINIO\_PASSWORD.

## **3\. 🚀 Deployment**

### **3.1 Build & Push Image**

Since this repo defines the execution environment for both the Code Location and the ephemeral Job Pods, you must build the image first.  
\# Build  
`docker build -t your-registry/dagster-crypto:latest .`

\# Push (Required for K8s to pull it)  
`docker push your-registry/dagster-crypto:latest`

### **3.2 Secrets Generation**

We use SealedSecrets to manage credentials.  
Create a .env file (do not commit this):  
```
DAGSTER\_POSTGRESQL\_PASSWORD=supersecret  
MINIO\_PASSWORD=anothersecret
```

Run the generation script:  
`sops exec-env .env "nu create\_sealed\_secrets.nu"`

### **3.3 Deploy to Cluster**

Update the image tag in apps/dagster/overlays/prod/kustomization.yaml if necessary, then deploy. This command uses Kustomize to inflate the Helm chart with your production patches.  
`kubectl apply -k apps/dagster/overlays/prod`

### **3.4 Verify Connection**

Check if the Dagster Platform has picked up the new location:

1. Open Dagster UI.  
2. Go to **Deployment** \> **Code Locations**.  
3. Ensure dagster-crypto is "Loaded" and green.

## **4\. Kubernetes Resources**

### **4.1 Resource Locations**

```
project/  
├── Dockerfile          \# Defines runtime environment  
├── apps/  
│   ├── dagster/  
│   │    ├── base/           \# Base Helm-based Kustomize config  
│   │    └── overlays/prod/  \# Production patches & SealedSecrets  
├── code/               \# Python business logic  
└── create\_sealed\_secrets.nu
```

## **5\. Configuration**

### **5.1 Environment Variables**

| Variable | Description |
| :---- | :---- |
| POSTGRESQL\_HOST | Database hostname. |
| MINIO\_ENDPOINT | MinIO API endpoint. |
| DAGSTER\_CURRENT\_IMAGE | Propagates the image version to job pods. |

### **5.2 Important Settings**

* **Replicas:** 1 (Stateless service).  
* **Resources:** 100m CPU / 256Mi RAM (For the gRPC server only).

## **6\. Networking**

### **6.1 Access URLs**

* **Web Interface:** http://dagster.homelab.lan (Served by the main Dagster platform).  
* **Internal gRPC:** dagster-crypto.dagster.svc.cluster.local:3030.

### **6.2 IngressRoute Configuration**

An ingress-route.yaml file exists in base/ for compatibility but is **not enabled** for this code location, as it is an internal service.

## **7\. Data Management**

### **7.1 Persistent Data Locations**

* **Raw Data:** MinIO bucket crypto-raw.  
* **Processed Data:** Postgres database crypto.

### **7.2 Backup Procedure**

**MinIO (Critical):**  
`mc mirror minio-local/crypto-raw /backup-location/`

**PostgreSQL:** Standard pg\_dump.

## **8\. Troubleshooting**

### **8.1 Common Issues**

* **Job Stuck in "Starting":** Usually indicates the Daemon cannot create the ephemeral pod. Check kubectl get events for ImagePullBackOff.  
* **Code Location Error:** If the Python code fails to load (syntax error), the Deployment will crash loop. Check kubectl logs.

### **8.2 Useful Commands**

\# View gRPC server logs  
`kubectl logs -n dagster -l app=dagster-crypto -f`

\# List running job pods  
`kubectl get pods -n dagster -l dagster/job`

## **9\. Maintenance**

### **9.1 Updating the Application**

1. Update code and rebuild/push the Docker image.  
2. Update the tag in kustomization.yaml.  
3. Apply changes:  
   `kubectl apply -k apps/dagster/overlays/prod`

4. Restart deployment to force code reload:  
   `kubectl rollout restart deployment -n dagster dagster-crypto`
