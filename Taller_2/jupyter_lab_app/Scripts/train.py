# Modules
import argparse
import os
import joblib
import pandas as pd
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from palmerpenguins import load_penguins
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split


class DataProcessor:
    """Handles all data loading, cleaning, transformation, and feature extraction operations."""
    
    def __init__(self):
        """Initialize DataProcessor.
        """
        self.df = None
        self.X = None
        self.y = None
    
    def load_data(self):
        """Load data using palmerpenguins load_penguins function."""
        self.df = load_penguins()
        return self
    
    def clean_data(self):
        """Remove missing values and duplicate rows."""
        self.df = self.df.dropna()
        self.df = self.df.drop_duplicates()
        return self
    
    def transform_data(self):
        """Transform species column to numeric and create dummy variables."""
        # Transform species column where Adelie=0, Chinstrap=1, Gentoo=2
        species_mapping = {'Adelie': 0, 'Chinstrap': 1, 'Gentoo': 2}
        self.df['species'] = self.df['species'].map(species_mapping)
        
        # Dummy encode categorical columns
        self.df = pd.get_dummies(self.df)
        return self
    
    def extract_features_target(self, target_column='species'):
        """Extract features and target variable from dataframe.
        
        Args:
            target_column (str): Name of the target column.
        """
        self.X = self.df.drop(columns=[target_column])
        self.y = self.df[target_column]
        return self
    
    def process(self, target_column='species'):
        """Run the complete data processing pipeline.
        
        Args:
            target_column (str): Name of the target column.
            
        Returns:
            tuple: X (features) and y (target) dataframes.
        """
        self.load_data()
        self.clean_data()
        self.transform_data()
        self.extract_features_target(target_column)
        return self.X, self.y


def split_data(X, y, test_size=0.3, val_size=0.5, random_state=42):
    """Split data into train, validation, and test sets.
    
    Args:
        X: Features dataframe.
        y: Target variable.
        test_size (float): Proportion of data for testing.
        val_size (float): Proportion of temp data for validation.
        random_state (int): Random seed for reproducibility.
        
    Returns:
        tuple: (X_train, X_val, X_test, y_train, y_val, y_test)
    """
    X_train, X_temp, y_train, y_temp = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp, y_temp, test_size=val_size, random_state=random_state
    )
    return X_train, X_val, X_test, y_train, y_val, y_test


class Model:
    """Handles model building, training, validation, and export operations."""
    
    def __init__(self, model_type='svm', **model_params):
        """Initialize Model with model type and parameters.
        
        Args:
            model_type (str): Type of model to build ('svm', 'logistic_regression', 'random_forest').
            **model_params: Additional parameters for the model.
        """
        self.model_type = model_type
        self.model_params = model_params
        self.model = None
        self.X_train = None
        self.X_val = None
        self.X_test = None
        self.y_train = None
        self.y_val = None
        self.y_test = None
    
    def set_data(self, X_train, X_val, X_test, y_train, y_val, y_test):
        """Set training, validation, and test data.
        
        Args:
            X_train: Training features.
            X_val: Validation features.
            X_test: Test features.
            y_train: Training target.
            y_val: Validation target.
            y_test: Test target.
        """
        self.X_train = X_train
        self.X_val = X_val
        self.X_test = X_test
        self.y_train = y_train
        self.y_val = y_val
        self.y_test = y_test
        return self
    
    def build_model(self):
        """Build model based on model type and parameters."""
        if self.model_type == 'svm':
            self.model = SVC(**self.model_params)
        elif self.model_type == 'logistic_regression':
            self.model = LogisticRegression(**self.model_params)
        elif self.model_type == 'random_forest':
            self.model = RandomForestClassifier(**self.model_params)
        else:
            raise ValueError(f'Unsupported model type: {self.model_type}')
        return self
    
    def train(self):
        """Train the model on training data."""
        if self.model is None:
            raise ValueError('Model not built. Call build_model() first.')
        self.model.fit(self.X_train, self.y_train)
        return self
    
    def validate(self):
        """Validate the model on validation data.
        
        Returns:
            str: Classification report.
        """
        if self.model is None:
            raise ValueError('Model not trained. Call train() first.')
        predictions = self.model.predict(self.X_val)
        report = classification_report(self.y_val, predictions)
        return report
    
    def test(self):
        """Test the model on test data.
        
        Returns:
            str: Classification report.
        """
        if self.model is None:
            raise ValueError('Model not trained. Call train() first.')
        predictions = self.model.predict(self.X_test)
        report = classification_report(self.y_test, predictions)
        return report
    
    def export(self, file_path):
        """Export trained model to file.
        
        Args:
            file_path (str): Path to save the model file.
        """
        if self.model is None:
            raise ValueError('Model not trained. Call train() first.')
        
        # Validate if folder exists, if not create it
        folder = os.path.dirname(file_path)
        if not os.path.exists(folder):
            os.makedirs(folder)
        
        joblib.dump(self.model, file_path)
        return self
    

if __name__ == '__main__':
    # Read Data_file and Model_file from command line arguments
    parser = argparse.ArgumentParser(description='Train models on the penguins dataset.')
    parser.add_argument(
        '--models_folder',
        type=str,
        help='Path to the output models folder.'
    )
    args = parser.parse_args()
    
    # ========== PREPROCESSING (DONE ONCE) ==========
    print(f'{"="*60}')
    print('DATA PREPROCESSING')
    print(f'{"="*60}')
    print('Loading data...')
    print('Cleaning data...')
    print('Transforming data...')
    print('Extracting features and target...')
    
    data_processor = DataProcessor()
    X, y = data_processor.process(target_column='species')
    
    print('Splitting data into train/validation/test sets...')
    X_train, X_val, X_test, y_train, y_val, y_test = split_data(X, y)
    print(f'Train samples: {len(X_train)}, Validation samples: {len(X_val)}, Test samples: {len(X_test)}')
    
    # Define model configurations
    model_configs = {
        'svm': {'kernel': 'rbf', 'C': 1.0},
        'logistic_regression': {'max_iter': 1000, 'random_state': 42},
        'random_forest': {'n_estimators': 100, 'random_state': 42}
    }
    
    # ========== TRAINING MODELS (USING SAME DATA SPLITS) ==========
    trained_models = {}
    
    for model_type, model_params in model_configs.items():
        print(f'\n{"="*60}')
        print(f'TRAINING {model_type.upper()} MODEL')
        print(f'{"="*60}')
        
        # Initialize model
        model = Model(model_type, **model_params)
        model.set_data(X_train, X_val, X_test, y_train, y_val, y_test)
        
        # Build and train
        print('Building model...')
        model.build_model()
        
        print('Training model...')
        model.train()
        
        # Validate
        print('Validating model...')
        validation_report = model.validate()
        print(validation_report)
        
        # Export
        model_file = os.path.join(args.models_folder, f'{model_type}.pkl')
        print(f'Exporting model to {model_file}...')
        model.export(model_file)
        
        # Store for testing
        trained_models[model_type] = model
    
    # ========== TESTING ALL MODELS ==========
    print(f'\n{"="*60}')
    print('TESTING ALL MODELS ON TEST DATASET')
    print(f'{"="*60}\n')
    
    for model_type, model in trained_models.items():
        print(f'\n{"="*60}')
        print(f'{model_type.upper()} - Test Results')
        print(f'{"="*60}')
        test_report = model.test()
        print(test_report)
    
    print(f'\n{"="*60}')
    print('ALL MODELS TRAINED AND TESTED SUCCESSFULLY!')
    print(f'Models saved in: {args.models_folder}')
    print(f'{"="*60}')