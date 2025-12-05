# GitHub Actions Setup for seqrneogen

This guide explains how to set up automatic Docker builds using GitHub Actions.

## Prerequisites

1. A Google Cloud service account with permissions to:
   - Build Docker images (Cloud Build)
   - Push to GCR (Container Registry)

## Setup Steps

### 1. Create a Service Account (if you don't have one)

```bash
gcloud iam service-accounts create github-actions-builder \
  --display-name="GitHub Actions Builder" \
  --project=seqrneogen

# Grant Cloud Build permissions
gcloud projects add-iam-policy-binding seqrneogen \
  --member="serviceAccount:github-actions-builder@seqrneogen.iam.gserviceaccount.com" \
  --role="roles/cloudbuild.builds.builder"

# Grant Storage permissions (for GCR)
gcloud projects add-iam-policy-binding seqrneogen \
  --member="serviceAccount:github-actions-builder@seqrneogen.iam.gserviceaccount.com" \
  --role="roles/storage.admin"
```

### 2. Create and Download Service Account Key

```bash
gcloud iam service-accounts keys create ~/github-actions-key.json \
  --iam-account=github-actions-builder@seqrneogen.iam.gserviceaccount.com \
  --project=seqrneogen
```

### 3. Add Secret to GitHub

1. Go to your GitHub repository: https://github.com/boscoliveira/seqr
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `GCP_SA_KEY`
5. Value: Copy the entire contents of `~/github-actions-key.json`
6. Click **Add secret**

### 4. Enable Required APIs

```bash
gcloud services enable cloudbuild.googleapis.com --project=seqrneogen
gcloud services enable containerregistry.googleapis.com --project=seqrneogen
```

## How It Works

When you push code to the `master` branch, GitHub Actions will:

1. ✅ Build a Docker image using Cloud Build
2. ✅ Push it to `gcr.io/seqrneogen/seqr:COMMIT_SHA` and `gcr.io/seqrneogen/seqr:latest`
3. ✅ Output the Helm command to deploy the new image

## Deploying After Build

After the GitHub Action completes, deploy the new image:

```bash
export COMMIT_SHA=<commit-sha-from-github-actions>
helm upgrade seqr seqr-helm/seqr-platform \
  --reuse-values \
  --set seqr.image.repository=gcr.io/seqrneogen/seqr \
  --set seqr.image.tag=${COMMIT_SHA} \
  -f seqr-values.yaml
```

Or use the `latest` tag:

```bash
helm upgrade seqr seqr-helm/seqr-platform \
  --reuse-values \
  --set seqr.image.repository=gcr.io/seqrneogen/seqr \
  --set seqr.image.tag=latest \
  -f seqr-values.yaml
```

## Optional: Auto-Deploy

To automatically deploy after build, you can add a deployment step to the workflow (requires GKE credentials).

