name: Daily News Pipeline

on:
  schedule:
    # Runs every day at 02:00 UTC (7:30 AM IST)
    - cron: "0 2 * * *"
  workflow_dispatch: # allows manual trigger from GitHub UI

env:
  GCP_PROJECT: ${{ secrets.GCP_PROJECT_ID }}
  GCP_REGION: us-central1
  JOB_NAME: news-pipeline-job
  IMAGE: gcr.io/${{ secrets.GCP_PROJECT_ID }}/news-pipeline:${{ github.sha }}

jobs:
  build-and-run:
    name: Build Image & Execute Cloud Run Job
    runs-on: ubuntu-latest
    timeout-minutes: 30

    permissions:
      contents: read
      id-token: write   # Required for Workload Identity Federation

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.GCP_WORKLOAD_IDENTITY_PROVIDER }}
          service_account: ${{ secrets.GCP_SERVICE_ACCOUNT }}

      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v2

      - name: Configure Docker for GCR
        run: gcloud auth configure-docker --quiet

      - name: Build & push Docker image
        run: |
          docker build -t $IMAGE ./scripts
          docker push $IMAGE

      - name: Update Cloud Run Job image
        run: |
          gcloud run jobs update $JOB_NAME \
            --image=$IMAGE \
            --region=$GCP_REGION \
            --project=$GCP_PROJECT

      - name: Execute Cloud Run Job
        run: |
          gcloud run jobs execute $JOB_NAME \
            --region=$GCP_REGION \
            --project=$GCP_PROJECT \
            --wait
