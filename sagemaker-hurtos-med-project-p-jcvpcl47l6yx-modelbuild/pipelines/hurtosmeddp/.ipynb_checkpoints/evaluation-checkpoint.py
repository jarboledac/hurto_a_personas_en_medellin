
import json
import os
import pathlib
import pickle as pkl
import tarfile
import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
import datetime as dt
from sklearn.metrics import roc_curve, auc
from sklearn.metrics import r2_score

if __name__ == "__main__":   
    
    # All paths are local for the processing container
    model_path = "/opt/ml/processing/model/model.tar.gz"
    test_x_path = "/opt/ml/processing/test/test_x.csv"
    test_y_path = "/opt/ml/processing/test/test_y.csv"
    output_dir = "/opt/ml/processing/evaluation"
    output_prediction_path = "/opt/ml/processing/output/"
        
    # Read model tar file
    with tarfile.open(model_path, "r:gz") as t:
        t.extractall(path=".")
    
    # Load model
    model = xgb.Booster()
    model.load_model("xgboost-model")
    
    # Read test data
    X_test = xgb.DMatrix(pd.read_csv(test_x_path).values)
    y_test = pd.read_csv(test_y_path).to_numpy()

    # Run predictions
    #probability = model.predict(X_test)
    y_hat = model.predict(X_test)

    # Evaluate predictions
    test_auc = r2_score(y_test, y_hat)
    #auc_score = auc(fpr, tpr)
    report_dict = {
        "regression_metrics": {
            "r2_score": {
                "value": test_auc,
            },
        },
    }

    # Save evaluation report
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    with open(f"{output_dir}/evaluation.json", "w") as f:
        f.write(json.dumps(report_dict))
    
    # Save prediction baseline file - we need it later for the model quality monitoring
    pd.DataFrame({"prediction":np.array(np.round(y_hat), dtype=int),
                  "probability":y_hat,
                  "label":y_test.squeeze()}
                ).to_csv(os.path.join(output_prediction_path, 'prediction_baseline/prediction_baseline.csv'), index=False, header=True)
