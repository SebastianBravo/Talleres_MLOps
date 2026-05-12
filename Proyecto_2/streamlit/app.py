import os

import requests
import streamlit as st

API_URL = os.environ.get("API_URL", "http://api:8000")

# Valores de ejemplo basados en un registro real del dataset
EXAMPLE_VALUES = {
    "race": "Caucasian",
    "gender": "Female",
    "age": "[70-80)",
    "weight": None,
    "admission_type_id": 1,
    "discharge_disposition_id": 3,
    "admission_source_id": 7,
    "time_in_hospital": 5,
    "payer_code": "MC",
    "medical_specialty": "InternalMedicine",
    "num_lab_procedures": 45,
    "num_procedures": 1,
    "num_medications": 18,
    "number_outpatient": 0,
    "number_emergency": 0,
    "number_inpatient": 1,
    "diag_1": "428",
    "diag_2": "250",
    "diag_3": "401",
    "number_diagnoses": 9,
    "max_glu_serum": "None",
    "a1cresult": "None",
    "metformin": "No",
    "repaglinide": "No",
    "nateglinide": "No",
    "chlorpropamide": "No",
    "glimepiride": "No",
    "acetohexamide": "No",
    "glipizide": "No",
    "glyburide": "No",
    "tolbutamide": "No",
    "pioglitazone": "No",
    "rosiglitazone": "No",
    "acarbose": "No",
    "miglitol": "No",
    "troglitazone": "No",
    "tolazamide": "No",
    "examide": "No",
    "citoglipton": "No",
    "insulin": "Steady",
    "glyburide_metformin": "No",
    "glipizide_metformin": "No",
    "glimepiride_pioglitazone": "No",
    "metformin_rosiglitazone": "No",
    "metformin_pioglitazone": "No",
    "change": "Ch",
    "diabetesmed": "Yes",
}

MEDICATION_OPTIONS = ["No", "Steady", "Up", "Down"]
MEDICATION_FIELDS = [
    "metformin", "repaglinide", "nateglinide", "chlorpropamide",
    "glimepiride", "acetohexamide", "glipizide", "glyburide",
    "tolbutamide", "pioglitazone", "rosiglitazone", "acarbose",
    "miglitol", "troglitazone", "tolazamide", "examide",
    "citoglipton", "insulin",
    "glyburide_metformin", "glipizide_metformin",
    "glimepiride_pioglitazone", "metformin_rosiglitazone",
    "metformin_pioglitazone",
]


def init_session_state():
    if "form_data" not in st.session_state:
        st.session_state.form_data = {k: v for k, v in EXAMPLE_VALUES.items()}


def load_example():
    st.session_state.form_data = {k: v for k, v in EXAMPLE_VALUES.items()}


def get_model_info():
    try:
        resp = requests.get(f"{API_URL}/model-info", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


# =========================
# Layout
# =========================

st.set_page_config(
    page_title="Diabetes Readmission",
    page_icon="🏥",
    layout="wide",
)

init_session_state()

st.title("Prediccion de Readmision Hospitalaria")
st.caption("Dataset: Diabetes 130-US Hospitals (1999-2008) — RandomForestClassifier via MLflow")

# --- Sidebar: info del modelo ---
with st.sidebar:
    st.header("Modelo activo")
    info = get_model_info()
    if info and info.get("model_ready"):
        model = info.get("model", {})
        st.success("Modelo cargado")
        st.write(f"**Nombre:** {model.get('name', '-')}")
        st.write(f"**Version:** {model.get('version', '-')}")
        st.write(f"**Alias:** {model.get('alias', '-')}")
        st.write(f"**Cargado:** {model.get('loaded_at', '-')}")
    elif info:
        st.warning("Modelo no disponible aun")
        st.caption(info.get("model_status", {}).get("message", ""))
    else:
        st.error("No se pudo conectar con la API")

    st.divider()
    st.caption(f"API: `{API_URL}`")

# --- Boton cargar ejemplo ---
col_btn, _ = st.columns([1, 4])
with col_btn:
    st.button("Cargar ejemplo", on_click=load_example, type="secondary")

st.divider()

# =========================
# Formulario de entrada
# =========================

v = st.session_state.form_data

with st.expander("Datos demograficos", expanded=True):
    c1, c2, c3, c4 = st.columns(4)
    v["race"] = c1.selectbox(
        "Raza", ["Caucasian", "AfricanAmerican", "Hispanic", "Asian", "Other", "?"],
        index=["Caucasian", "AfricanAmerican", "Hispanic", "Asian", "Other", "?"].index(v["race"] or "Caucasian"),
    )
    v["gender"] = c2.selectbox(
        "Genero", ["Male", "Female", "Unknown/Invalid"],
        index=["Male", "Female", "Unknown/Invalid"].index(v["gender"] or "Female"),
    )
    age_options = [
        "[0-10)", "[10-20)", "[20-30)", "[30-40)", "[40-50)",
        "[50-60)", "[60-70)", "[70-80)", "[80-90)", "[90-100)",
    ]
    v["age"] = c3.selectbox(
        "Edad", age_options,
        index=age_options.index(v["age"] or "[70-80)"),
    )
    v["weight"] = c4.text_input("Peso (dejar vacio = desconocido)", value=v["weight"] or "")
    if v["weight"] == "":
        v["weight"] = None

with st.expander("Informacion de admision", expanded=True):
    c1, c2, c3 = st.columns(3)
    v["admission_type_id"] = c1.number_input(
        "Tipo de admision (ID)", min_value=1, max_value=8, step=1,
        value=int(v["admission_type_id"] or 1),
    )
    v["discharge_disposition_id"] = c2.number_input(
        "Disposicion al alta (ID)", min_value=1, max_value=30, step=1,
        value=int(v["discharge_disposition_id"] or 1),
    )
    v["admission_source_id"] = c3.number_input(
        "Fuente de admision (ID)", min_value=1, max_value=25, step=1,
        value=int(v["admission_source_id"] or 7),
    )
    c4, c5, c6 = st.columns(3)
    v["time_in_hospital"] = c4.number_input(
        "Dias en hospital", min_value=1, max_value=14, step=1,
        value=int(v["time_in_hospital"] or 1),
    )
    v["payer_code"] = c5.text_input("Codigo pagador", value=v["payer_code"] or "")
    v["medical_specialty"] = c6.text_input(
        "Especialidad medica", value=v["medical_specialty"] or "",
    )

with st.expander("Laboratorio y diagnosticos", expanded=True):
    c1, c2, c3, c4 = st.columns(4)
    v["num_lab_procedures"] = c1.number_input(
        "Procedimientos de lab", min_value=0, step=1,
        value=int(v["num_lab_procedures"] or 0),
    )
    v["num_procedures"] = c2.number_input(
        "Procedimientos", min_value=0, step=1,
        value=int(v["num_procedures"] or 0),
    )
    v["num_medications"] = c3.number_input(
        "Medicamentos", min_value=0, step=1,
        value=int(v["num_medications"] or 0),
    )
    v["number_diagnoses"] = c4.number_input(
        "Num. diagnosticos", min_value=0, step=1,
        value=int(v["number_diagnoses"] or 0),
    )
    c5, c6, c7 = st.columns(3)
    v["number_outpatient"] = c5.number_input(
        "Visitas ambulatorias", min_value=0, step=1,
        value=int(v["number_outpatient"] or 0),
    )
    v["number_emergency"] = c6.number_input(
        "Visitas emergencia", min_value=0, step=1,
        value=int(v["number_emergency"] or 0),
    )
    v["number_inpatient"] = c7.number_input(
        "Hospitalizaciones", min_value=0, step=1,
        value=int(v["number_inpatient"] or 0),
    )
    c8, c9, c10 = st.columns(3)
    v["diag_1"] = c8.text_input("Diagnostico 1", value=v["diag_1"] or "")
    v["diag_2"] = c9.text_input("Diagnostico 2", value=v["diag_2"] or "")
    v["diag_3"] = c10.text_input("Diagnostico 3", value=v["diag_3"] or "")

    c11, c12 = st.columns(2)
    glu_options = ["None", "Norm", ">200", ">300"]
    v["max_glu_serum"] = c11.selectbox(
        "Glucosa en suero max", glu_options,
        index=glu_options.index(v["max_glu_serum"] or "None"),
    )
    a1c_options = ["None", "Norm", ">7", ">8"]
    v["a1cresult"] = c12.selectbox(
        "Resultado A1C", a1c_options,
        index=a1c_options.index(v["a1cresult"] or "None"),
    )

with st.expander("Medicamentos", expanded=False):
    cols = st.columns(4)
    for idx, field in enumerate(MEDICATION_FIELDS):
        col = cols[idx % 4]
        current = v.get(field) or "No"
        if current not in MEDICATION_OPTIONS:
            current = "No"
        v[field] = col.selectbox(
            field.replace("_", "-"),
            MEDICATION_OPTIONS,
            index=MEDICATION_OPTIONS.index(current),
            key=f"med_{field}",
        )

with st.expander("Otros", expanded=True):
    c1, c2 = st.columns(2)
    v["change"] = c1.selectbox(
        "Cambio de medicacion", ["No", "Ch"],
        index=["No", "Ch"].index(v["change"] or "No"),
    )
    v["diabetesmed"] = c2.selectbox(
        "Medicamento para diabetes", ["Yes", "No"],
        index=["Yes", "No"].index(v["diabetesmed"] or "Yes"),
    )

st.divider()

# =========================
# Prediccion
# =========================

if st.button("Predecir", type="primary", use_container_width=True):

    # Construir payload — campos vacíos se envían como null
    payload = {}
    for key, val in v.items():
        if val == "":
            payload[key] = None
        else:
            payload[key] = val

    try:
        resp = requests.post(f"{API_URL}/predict", json=payload, timeout=15)

        if resp.status_code == 200:
            result = resp.json()
            prediction = result.get("prediction", "-")

            st.divider()

            # Resultado prominente
            if prediction == "<30":
                color, label = "#c0392b", "Readmision en menos de 30 dias"
            elif prediction == ">30":
                color, label = "#e67e22", "Readmision en mas de 30 dias"
            else:
                color, label = "#27ae60", "Sin readmision prevista"

            st.markdown(
                f"""
                <div style="background-color:{color}22; border-left: 6px solid {color};
                            padding: 1.2rem 1.5rem; border-radius: 6px; margin-bottom: 1rem;">
                    <span style="font-size: 0.85rem; color:{color}; font-weight:600;
                                 text-transform: uppercase; letter-spacing: 0.05em;">Prediccion</span>
                    <div style="font-size: 2rem; font-weight: 700; color:{color};
                                margin-top: 0.2rem;">{label}</div>
                    <div style="font-size: 1.1rem; color:{color}; opacity:0.8;">Clase: {prediction}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Probabilidades
            probabilities = result.get("probabilities")
            if probabilities:
                st.subheader("Probabilidades por clase")

                col_metrics, col_chart = st.columns([1, 2])

                with col_metrics:
                    for cls, prob in probabilities.items():
                        st.metric(label=cls, value=f"{prob:.1%}")
                        st.progress(prob)

                with col_chart:
                    import pandas as pd
                    chart_data = pd.DataFrame(
                        {"Probabilidad": list(probabilities.values())},
                        index=list(probabilities.keys()),
                    )
                    st.bar_chart(chart_data, height=220)

            # Informacion del modelo y respuesta
            st.divider()
            st.subheader("Informacion de la inferencia")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Modelo", result.get("model_name", "-"))
            m2.metric("Version", result.get("model_version", "-"))
            m3.metric("Alias", result.get("model_alias", "-"))
            m4.metric("Tiempo de respuesta", f"{result.get('response_time_ms', 0):.1f} ms")

        elif resp.status_code == 503:
            st.warning(
                "El modelo aun no esta disponible. "
                "Espera a que el DAG de Airflow complete el primer entrenamiento."
            )

        elif resp.status_code == 422:
            st.error("Error de validacion en los datos enviados.")
            st.json(resp.json())

        else:
            st.error(f"Error de la API ({resp.status_code})")
            st.json(resp.json())

    except requests.exceptions.ConnectionError:
        st.error(
            f"No se pudo conectar con la API en `{API_URL}`. "
            "Verifica que el servicio este corriendo."
        )
    except requests.exceptions.Timeout:
        st.error("La API tardó demasiado en responder (timeout 15s).")
    except Exception as exc:
        st.error(f"Error inesperado: {exc}")
