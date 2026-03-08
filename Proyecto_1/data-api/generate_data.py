"""Generate covertype.csv in the raw format expected by the Data API."""
import csv
from sklearn.datasets import fetch_covtype
import numpy as np

WILDERNESS_AREAS = ["Rawah", "Neota", "Comanche Peak", "Cache la Poudre"]

SOIL_TYPES = [
    "C2702", "C2703", "C2704", "C2705", "C2706", "C2717",
    "C3501", "C3502", "C4201", "C4703", "C4704", "C4744",
    "C4758", "C5101", "C5151", "C6101", "C6102", "C6731",
    "C7101", "C7102", "C7103", "C7201", "C7202", "C7700",
    "C7701", "C7702", "C7709", "C7710", "C7745", "C7746",
    "C7755", "C7756", "C7757", "C7790", "C8703", "C8707",
    "C8708", "C8771", "C8772", "C8776",
]

HEADER = [
    "Elevation", "Aspect", "Slope",
    "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
    "Horizontal_Distance_To_Roadways",
    "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
    "Horizontal_Distance_To_Fire_Points",
    "Wilderness_Area", "Soil_Type", "Cover_Type",
]


def one_hot_to_label(one_hot_cols, labels):
    idx = np.argmax(one_hot_cols, axis=1)
    return [labels[i] for i in idx]


def main():
    print("Downloading covtype dataset...")
    covtype = fetch_covtype(as_frame=False)
    X, y = covtype.data, covtype.target

    quantitative = X[:, :10].astype(int)
    wilderness_oh = X[:, 10:14]
    soil_oh = X[:, 14:54]

    wilderness_labels = one_hot_to_label(wilderness_oh, WILDERNESS_AREAS)
    soil_labels = one_hot_to_label(soil_oh, SOIL_TYPES)

    print(f"Writing {len(y)} rows to data/covertype.csv...")
    with open("data/covertype.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        for i in range(len(y)):
            row = list(quantitative[i]) + [wilderness_labels[i], soil_labels[i], int(y[i])]
            writer.writerow(row)

    print("Done.")


if __name__ == "__main__":
    main()
