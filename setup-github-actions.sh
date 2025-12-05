#!/bin/bash
set -e

PROJECT_ID="seqrneogen"
SA_NAME="github-actions-builder"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
KEY_FILE="$HOME/github-actions-key.json"

echo "=== Setting up GitHub Actions for seqrneogen ==="
echo ""

# Step 1: Create service account
echo "Step 1: Creating service account..."
gcloud iam service-accounts create ${SA_NAME} \
  --display-name="GitHub Actions Builder" \
  --project=${PROJECT_ID} 2>/dev/null || echo "Service account already exists"

# Step 2: Grant permissions
echo ""
echo "Step 2: Granting permissions..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/cloudbuild.builds.builder" \
  --condition=None 2>/dev/null || echo "Cloud Build role already granted"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.admin" \
  --condition=None 2>/dev/null || echo "Storage role already granted"

# Step 3: Create key
echo ""
echo "Step 3: Creating service account key..."
if [ -f "${KEY_FILE}" ]; then
  echo "⚠️  Key file already exists at ${KEY_FILE}"
  read -p "Overwrite? (y/N): " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Skipping key creation"
    exit 0
  fi
fi

gcloud iam service-accounts keys create ${KEY_FILE} \
  --iam-account=${SA_EMAIL} \
  --project=${PROJECT_ID}

# Step 4: Enable APIs
echo ""
echo "Step 4: Enabling required APIs..."
gcloud services enable cloudbuild.googleapis.com --project=${PROJECT_ID} 2>/dev/null || echo "Cloud Build API already enabled"
gcloud services enable containerregistry.googleapis.com --project=${PROJECT_ID} 2>/dev/null || echo "Container Registry API already enabled"

# Step 5: Display key
echo ""
echo "=== Setup Complete! ==="
echo ""
echo "Service account key created at: ${KEY_FILE}"
echo ""
echo "Next steps:"
echo "1. Go to: https://github.com/boscoliveira/seqr/settings/secrets/actions"
echo "2. Click 'New repository secret'"
echo "3. Name: GCP_SA_KEY"
echo "4. Value: Copy the entire contents of ${KEY_FILE}"
echo ""
echo "To view the key:"
echo "  cat ${KEY_FILE}"
