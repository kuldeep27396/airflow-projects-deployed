"""
Machine Learning Pipeline DAG Example
======================================

This DAG demonstrates a complete ML pipeline workflow:
- Data preparation and validation
- Feature engineering
- Model training and evaluation
- Model deployment simulation
- Monitoring and alerting

This example uses scikit-learn for simplicity, but concepts apply to any ML framework.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
try:
    from sklearn.datasets import make_classification
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score, classification_report
    import joblib
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
import json
import os

def generate_sample_data(**context):
    """Generate sample dataset for ML training."""
    print("=== Generating Sample Dataset ===")
    
    if not SKLEARN_AVAILABLE:
        print("⚠️  scikit-learn not available, generating mock data")
        # Create mock dataset
        np.random.seed(42)
        X = np.random.randn(1000, 20)
        y = np.random.randint(0, 2, 1000)
    else:
        # Create synthetic classification dataset
        X, y = make_classification(
            n_samples=1000,
            n_features=20,
            n_informative=15,
            n_redundant=5,
            n_clusters_per_class=1,
            random_state=42
        )
    
    # Create feature names
    feature_names = [f'feature_{i}' for i in range(X.shape[1])]
    
    # Create DataFrame
    df = pd.DataFrame(X, columns=feature_names)
    df['target'] = y
    
    # Add some noise and missing values for realism
    np.random.seed(42)
    noise_indices = np.random.choice(df.index, size=50, replace=False)
    df.loc[noise_indices, 'feature_0'] = np.nan
    
    # Save to CSV
    data_path = '/tmp/ml_training_data.csv'
    df.to_csv(data_path, index=False)
    
    print(f"Generated dataset with {len(df)} samples and {len(feature_names)} features")
    print(f"Target distribution: {df['target'].value_counts().to_dict()}")
    print(f"Dataset saved to: {data_path}")
    
    return data_path

def validate_data():
    """Validate the dataset for quality issues."""
    print("=== Data Validation ===")
    
    data_path = '/tmp/ml_training_data.csv'
    df = pd.read_csv(data_path)
    
    validation_results = {
        'total_rows': len(df),
        'total_features': len(df.columns) - 1,  # Exclude target
        'missing_values': df.isnull().sum().sum(),
        'duplicate_rows': df.duplicated().sum(),
        'target_distribution': df['target'].value_counts().to_dict()
    }
    
    print(f"Dataset shape: {df.shape}")
    print(f"Missing values: {validation_results['missing_values']}")
    print(f"Duplicate rows: {validation_results['duplicate_rows']}")
    print(f"Target distribution: {validation_results['target_distribution']}")
    
    # Save validation results
    validation_path = '/tmp/data_validation_results.json'
    with open(validation_path, 'w') as f:
        json.dump(validation_results, f, indent=2)
    
    # Basic validation checks
    if validation_results['missing_values'] > len(df) * 0.1:  # More than 10% missing
        print("WARNING: High percentage of missing values detected")
    
    if len(set(validation_results['target_distribution'].values())) > 2:
        imbalance_ratio = max(validation_results['target_distribution'].values()) / min(validation_results['target_distribution'].values())
        if imbalance_ratio > 3:
            print(f"WARNING: Class imbalance detected (ratio: {imbalance_ratio:.2f})")
    
    print("✓ Data validation completed")

def prepare_features():
    """Prepare features for training."""
    print("=== Feature Engineering ===")
    
    data_path = '/tmp/ml_training_data.csv'
    df = pd.read_csv(data_path)
    
    # Handle missing values
    df = df.fillna(df.mean())
    
    # Feature engineering - create some derived features
    df['feature_sum'] = df[['feature_0', 'feature_1', 'feature_2']].sum(axis=1)
    df['feature_mean'] = df[['feature_0', 'feature_1', 'feature_2']].mean(axis=1)
    df['feature_std'] = df[['feature_0', 'feature_1', 'feature_2']].std(axis=1)
    
    # Feature scaling (simple standardization)
    feature_cols = [col for col in df.columns if col.startswith('feature_')]
    for col in feature_cols:
        df[col] = (df[col] - df[col].mean()) / df[col].std()
    
    # Save processed data
    processed_path = '/tmp/ml_processed_data.csv'
    df.to_csv(processed_path, index=False)
    
    print(f"Feature engineering completed")
    print(f"Total features after engineering: {len(feature_cols)}")
    print(f"Processed data saved to: {processed_path}")

def train_model(**context):
    """Train the machine learning model."""
    print("=== Model Training ===")
    
    # Load processed data
    data_path = '/tmp/ml_processed_data.csv'
    df = pd.read_csv(data_path)
    
    # Separate features and target
    feature_cols = [col for col in df.columns if col.startswith('feature_')]
    X = df[feature_cols]
    y = df['target']
    
    if not SKLEARN_AVAILABLE:
        print("⚠️  scikit-learn not available, using mock training")
        # Mock training process
        print(f"Mock training set size: {int(len(X) * 0.8)}")
        print(f"Mock test set size: {int(len(X) * 0.2)}")
        
        # Simulate training
        accuracy = 0.85  # Mock accuracy
        print(f"Mock model accuracy: {accuracy:.4f}")
        
        # Mock feature importance
        feature_importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': np.random.rand(len(feature_cols))
        }).sort_values('importance', ascending=False)
        
        print("\nTop 10 Most Important Features (Mock):")
        print(feature_importance.head(10))
        
        # Save mock model info
        model_path = '/tmp/trained_model.json'
        with open(model_path, 'w') as f:
            json.dump({'type': 'mock_model', 'accuracy': accuracy}, f)
    else:
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        print(f"Training set size: {len(X_train)}")
        print(f"Test set size: {len(X_test)}")
        
        # Train model
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        
        model.fit(X_train, y_train)
        
        # Make predictions
        y_pred = model.predict(X_test)
        
        # Calculate metrics
        accuracy = accuracy_score(y_test, y_pred)
        
        print(f"Model accuracy: {accuracy:.4f}")
        print("\nClassification Report:")
        print(classification_report(y_test, y_pred))
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print("\nTop 10 Most Important Features:")
        print(feature_importance.head(10))
        
        # Save model
        model_path = '/tmp/trained_model.joblib'
        joblib.dump(model, model_path)
    
    # Save metrics
    metrics = {
        'accuracy': accuracy,
        'training_samples': len(X_train),
        'test_samples': len(X_test),
        'feature_count': len(feature_cols),
        'model_type': 'RandomForestClassifier',
        'training_date': context['ds']
    }
    
    metrics_path = '/tmp/model_metrics.json'
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"Model saved to: {model_path}")
    print(f"Metrics saved to: {metrics_path}")

def evaluate_model():
    """Evaluate the trained model and generate evaluation report."""
    print("=== Model Evaluation ===")
    
    # Load model and data
    df = pd.read_csv('/tmp/ml_processed_data.csv')
    
    if not SKLEARN_AVAILABLE:
        print("⚠️  scikit-learn not available, using mock evaluation")
        # Mock evaluation
        accuracy = 0.85
        evaluation_results = {
            'overall_accuracy': accuracy,
            'prediction_confidence_avg': 0.82,
            'low_confidence_predictions': 45,
            'class_predictions': {'0': 485, '1': 515}
        }
    else:
        # Load actual model
        model = joblib.load('/tmp/trained_model.joblib')
    
    feature_cols = [col for col in df.columns if col.startswith('feature_')]
    X = df[feature_cols]
    y = df['target']
    
    if SKLEARN_AVAILABLE:
        # Make predictions on full dataset
        y_pred = model.predict(X)
        y_pred_proba = model.predict_proba(X)
        
        # Calculate various metrics
        accuracy = accuracy_score(y, y_pred)
        
        evaluation_results = {
            'overall_accuracy': accuracy,
            'prediction_confidence_avg': y_pred_proba.max(axis=1).mean(),
            'low_confidence_predictions': (y_pred_proba.max(axis=1) < 0.7).sum(),
            'class_predictions': {
                str(class_): int(count) 
                for class_, count in pd.Series(y_pred).value_counts().items()
            }
        }
    
    print(f"Overall accuracy: {evaluation_results['overall_accuracy']:.4f}")
    print(f"Average prediction confidence: {evaluation_results['prediction_confidence_avg']:.4f}")
    print(f"Low confidence predictions: {evaluation_results['low_confidence_predictions']}")
    print(f"Class predictions: {evaluation_results['class_predictions']}")
    
    # Save evaluation results
    eval_path = '/tmp/model_evaluation.json'
    with open(eval_path, 'w') as f:
        json.dump(evaluation_results, f, indent=2)
    
    print(f"Evaluation results saved to: {eval_path}")

def deploy_model_simulation():
    """Simulate model deployment process."""
    print("=== Model Deployment Simulation ===")
    
    # Load model metrics
    with open('/tmp/model_metrics.json', 'r') as f:
        metrics = json.load(f)
    
    print("Deploying model to production environment...")
    print(f"Model type: {metrics['model_type']}")
    print(f"Accuracy: {metrics['accuracy']:.4f}")
    print(f"Training date: {metrics['training_date']}")
    
    # Simulate deployment steps
    deployment_steps = [
        "✓ Model validation passed",
        "✓ Model serialization completed", 
        "✓ Uploading to model registry",
        "✓ Creating model endpoint",
        "✓ Running health checks",
        "✓ Updating load balancer",
        "✓ Model deployment completed"
    ]
    
    for step in deployment_steps:
        print(step)
    
    # Create deployment manifest
    deployment_info = {
        'model_version': f"v{metrics['training_date'].replace('-', '')}",
        'deployment_date': datetime.now().isoformat(),
        'model_metrics': metrics,
        'deployment_status': 'success'
    }
    
    deployment_path = '/tmp/deployment_manifest.json'
    with open(deployment_path, 'w') as f:
        json.dump(deployment_info, f, indent=2)
    
    print(f"Deployment manifest saved to: {deployment_path}")

def model_monitoring():
    """Simulate model monitoring and alerting."""
    print("=== Model Monitoring ===")
    
    # Load evaluation results
    with open('/tmp/model_evaluation.json', 'r') as f:
        eval_results = json.load(f)
    
    print("Monitoring model performance...")
    
    # Simulate monitoring checks
    accuracy_threshold = 0.8
    confidence_threshold = 0.75
    
    alerts = []
    
    if eval_results['overall_accuracy'] < accuracy_threshold:
        alerts.append(f"LOW ACCURACY: {eval_results['overall_accuracy']:.4f} < {accuracy_threshold}")
    
    if eval_results['prediction_confidence_avg'] < confidence_threshold:
        alerts.append(f"LOW CONFIDENCE: {eval_results['prediction_confidence_avg']:.4f} < {confidence_threshold}")
    
    if eval_results['low_confidence_predictions'] > 100:
        alerts.append(f"HIGH LOW-CONFIDENCE PREDICTIONS: {eval_results['low_confidence_predictions']}")
    
    if alerts:
        print("🚨 ALERTS DETECTED:")
        for alert in alerts:
            print(f"  - {alert}")
    else:
        print("✅ All monitoring checks passed")
    
    # Create monitoring report
    monitoring_report = {
        'monitoring_date': datetime.now().isoformat(),
        'alerts': alerts,
        'metrics': eval_results,
        'status': 'healthy' if not alerts else 'attention_required'
    }
    
    monitoring_path = '/tmp/monitoring_report.json'
    with open(monitoring_path, 'w') as f:
        json.dump(monitoring_report, f, indent=2)
    
    print(f"Monitoring report saved to: {monitoring_path}")

def cleanup_ml_artifacts():
    """Clean up ML artifacts and temporary files."""
    ml_files = [
        '/tmp/ml_training_data.csv',
        '/tmp/ml_processed_data.csv',
        '/tmp/trained_model.joblib',
        '/tmp/trained_model.json',  # Mock model file
        '/tmp/model_metrics.json',
        '/tmp/model_evaluation.json',
        '/tmp/data_validation_results.json',
        '/tmp/deployment_manifest.json',
        '/tmp/monitoring_report.json'
    ]
    
    cleaned_files = []
    for file_path in ml_files:
        if os.path.exists(file_path):
            os.remove(file_path)
            cleaned_files.append(file_path)
    
    print(f"Cleaned up {len(cleaned_files)} ML artifacts")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'ml_pipeline_dag',
    default_args=default_args,
    schedule=None,  # Manual trigger for testing
    catchup=False,
    tags=["learning", "ml", "pipeline", "advanced"]
)

# Data preparation tasks
generate_data_task = PythonOperator(
    task_id='generate_sample_data',
    python_callable=generate_sample_data,
    dag=dag
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)

prepare_features_task = PythonOperator(
    task_id='prepare_features',
    python_callable=prepare_features,
    dag=dag
)

# Model training tasks
train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag
)

evaluate_model_task = PythonOperator(
    task_id='evaluate_model',
    python_callable=evaluate_model,
    dag=dag
)

# Deployment tasks
deploy_model_task = PythonOperator(
    task_id='deploy_model_simulation',
    python_callable=deploy_model_simulation,
    dag=dag
)

# Monitoring tasks
monitoring_task = PythonOperator(
    task_id='model_monitoring',
    python_callable=model_monitoring,
    dag=dag
)

# Cleanup task
cleanup_task = PythonOperator(
    task_id='cleanup_ml_artifacts',
    python_callable=cleanup_ml_artifacts,
    dag=dag
)

# Define dependencies
generate_data_task >> validate_data_task >> prepare_features_task
prepare_features_task >> train_model_task >> evaluate_model_task
evaluate_model_task >> deploy_model_task >> monitoring_task >> cleanup_task