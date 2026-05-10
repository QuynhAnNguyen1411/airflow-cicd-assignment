# Assignment 2 — CI/CD Pipeline for Airflow DAG

## Student
- GitHub: https://github.com/QuynhAnNguyen1411/airflow-cicd-assignment
- GCP Project: healthcare-analytics-495602

## Checklist
- [x] GitHub repo với đủ cấu trúc yêu cầu
- [x] cloudbuild.yaml với 4 steps: lint → test → deploy → trigger
- [x] Tests chỉ pass thì DAG mới deploy (flake8 + pytest)
- [x] Cloud Build trigger tự động khi push to main
- [x] DAG deploy lên Cloud Composer tự động
- [x] DAG trigger chạy tự động sau deploy
- [x] Logs + evidence trong evidence/

## Flow
git push → Cloud Build trigger → lint (flake8) → test (pytest 4 tests) → deploy DAG (gsutil cp) → trigger DAG (gcloud composer) → Airflow runs Bronze→Silver→Gold

## Build thành công
- Build ID: 7745887c (2 min 47 sec, tất cả 4 steps PASSED)

## Resources
- Cloud Composer 3 Small (asia-southeast1)
- Cloud Build trigger: deploy-dag-on-push-main
- DAG: healthcare_etl_pipeline (7 BQ tasks)
