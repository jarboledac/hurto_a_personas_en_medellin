
import pandas as pd
import numpy as np
import argparse
import os

def _parse_args():
    
    parser = argparse.ArgumentParser()
    # Data, model, and output directories
    # model_dir is always passed in from SageMaker. By default this is a S3 path under the default bucket.
    parser.add_argument('--filepath', type=str, default='/opt/ml/processing/input/')
    parser.add_argument('--filename', type=str, default='hurtos_med.csv')
    parser.add_argument('--outputpath', type=str, default='/opt/ml/processing/output/')
    
    return parser.parse_known_args()


if __name__=="__main__":
    # Process arguments
    args, _ = _parse_args()
    
    target_col = "nro_casos"
    
    # Load data
    df_data = pd.read_csv(os.path.join(args.filepath, args.filename), sep=",", index_col = 0)

    df_model_data = df_data.loc[:, ~df_data.columns.isin(['comuna','barrio','geometry','key','shape__are'])]

    # Shuffle and splitting dataset
    train_data, validation_data, test_data = np.split(
        df_model_data.sample(frac=1, random_state=1729),
        [int(0.8 * len(df_model_data)), int(0.9 * len(df_model_data))],
    )

    print(f"Data split > train:{train_data.shape} | validation:{validation_data.shape} | test:{test_data.shape}")
    
    # Save datasets locally
    train_data.to_csv(os.path.join(args.outputpath, 'train/train.csv'), index=False)
    validation_data.to_csv(os.path.join(args.outputpath, 'validation/validation.csv'), index=False)
    test_data[target_col].to_csv(os.path.join(args.outputpath, 'test/test_y.csv'), index=False)
    test_data.drop([target_col], axis=1).to_csv(os.path.join(args.outputpath, 'test/test_x.csv'), index=False)
    
    # Save the baseline dataset for model monitoring
    df_model_data.drop([target_col], axis=1).to_csv(os.path.join(args.outputpath, 'baseline/baseline.csv'), index=False, header=False)
    
    print("## Processing complete. Exiting.")
