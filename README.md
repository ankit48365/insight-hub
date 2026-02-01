![CurrentLocal](https://img.shields.io/badge/machine-GoogleCloud-brightgreen)

#### to run code in gcloud editor 
dont use .py and dont use ./ 

`uv run -m other.refresh_acc_token_gsm`

#### update requirements.txt

`uv run -- uv export --format requirements-txt --no-hashes > requirements.txt`

#### code deploy 

`gcloud run jobs deploy insight-hub-cloud-job --source . --region northamerica-northeast1 --tasks 1 --project mystrava-464501 --max-retries 0`