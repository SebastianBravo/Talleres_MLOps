# import os
# import joblib
# from sklearn.svm import SVC
# from sklearn.linear_model import LogisticRegression
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.model_selection import train_test_split
# from sklearn.metrics import classification_report, accuracy_score

# # Models without scaler since data is already preprocessed
# MODEL_CONFIGS = {
#     "svm": SVC(kernel="rbf", C=1.0),
#     "logistic_regression": LogisticRegression(max_iter=1000, random_state=42),
#     "random_forest": RandomForestClassifier(n_estimators=100, random_state=42),
# }


# def split_data(df, target="species", test_size=0.3, random_state=42):
#     X = df.drop(columns=[target])
#     y = df[target]
#     X_train, X_test, y_train, y_test = train_test_split(
#         X, y, test_size=test_size, random_state=random_state
#     )
#     print(f"Train: {len(X_train)} | Test: {len(X_test)}")
#     return X_train, X_test, y_train, y_test


# def train_and_evaluate(model, X_train, X_test, y_train, y_test):
#     model.fit(X_train, y_train)
#     y_pred = model.predict(X_test)
#     accuracy = accuracy_score(y_test, y_pred)
#     report = classification_report(y_test, y_pred, zero_division=0)
#     print(f"Accuracy: {accuracy:.4f}")
#     print(f"\n{report}")
#     return model


# def save_model(model, models_path, name):
#     os.makedirs(models_path, exist_ok=True)
#     model_file = os.path.join(models_path, f"{name}.pkl")
#     joblib.dump(model, model_file)
#     print(f"Modelo guardado en: {model_file}")
