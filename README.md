Fraud Data Detection based on RandomForest Model  
Using semi-supervised model based on RandomForest to detect fraud data which contains positive data and unlabeled data.
---
Project Structure:  
- main.sh: Entry file
- es.py: 
  - Elasticsearch Utility Class, read and write data
- oss-tool.jar：
  - OSS Utility, upload model file into OSS and generate temporary download link
- response.py
  - Request Utility, send model download link to backend server by POST request
- train_model.py
  - Using sklearn to train semi-supervised model based on RandomForest
- train_model_dztt.py
  - Like train_model.py focus on different business
- write_testres_to_es.py
  - Using es.py write model result into Elasticsearch