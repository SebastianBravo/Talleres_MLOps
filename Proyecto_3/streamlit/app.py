import os
from datetime import date

import requests
import streamlit as st

API_URL = os.environ.get("API_URL", "http://api:8000")

# ─── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Real Estate Price Prediction",
    page_icon="🏡",
    layout="wide",
)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get(path, timeout=5):
    try:
        r = requests.get(f"{API_URL}{path}", timeout=timeout)
        return r if r.status_code == 200 else None
    except Exception:
        return None


def get_model_info():
    r = _get("/model-info")
    return r.json() if r else None


def get_batch_history():
    r = _get("/history", timeout=10)
    if r is None:
        return None
    return r.json().get("batches", [])


# ─── Session state ────────────────────────────────────────────────────────────

EXAMPLE = {
    "bed": 3,
    "bath": 2.0,
    "acre_lot": 0.12,
    "house_size": 1850.0,
    "prev_sold_date": "2021-08-15",
    "brokered_by": "Realty Group Inc",
    "street": "123 Maple St",
    "city": "Austin",
    "state": "Texas",
    "zip_code": "78701",
    "status": "for_sale",
}

if "form" not in st.session_state:
    st.session_state.form = {**EXAMPLE}

def _load_example():
    st.session_state.form = {**EXAMPLE}

# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Modelo activo")
    info = get_model_info()
    if info and info.get("model_ready"):
        m = info.get("model", {})
        st.success("Modelo cargado")
        st.write(f"**Nombre:** {m.get('name', '-')}")
        st.write(f"**Version:** {m.get('version', '-')}")
        st.write(f"**Alias:** {m.get('alias', '-')}")
        loaded_at = m.get("loaded_at", "")
        st.write(f"**Cargado:** {loaded_at[:19] if loaded_at else '-'}")
    elif info:
        st.warning("Modelo no disponible aun")
        st.caption(info.get("model_status", {}).get("message", ""))
    else:
        st.error("No se pudo conectar con la API")
    st.divider()
    st.caption(f"API: `{API_URL}`")

# ─── Title ────────────────────────────────────────────────────────────────────

st.title("Real Estate Price Prediction")
st.caption("Prediccion de precios de propiedades inmobiliarias — RandomForestRegressor via MLflow")

# ─── Tabs ─────────────────────────────────────────────────────────────────────

tab_inference, tab_history = st.tabs(["Inferencia", "Historial de entrenamiento y despliegue"])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Inferencia
# ═══════════════════════════════════════════════════════════════════════════════

STATUS_OPTIONS = ["for_sale", "ready_to_build", "sold"]

with tab_inference:
    col_btn, _ = st.columns([1, 5])
    with col_btn:
        st.button("Cargar ejemplo", on_click=_load_example)

    f = st.session_state.form

    with st.expander("Caracteristicas fisicas", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        f["bed"]        = c1.number_input("Habitaciones", min_value=0, max_value=20, step=1,   value=int(f["bed"]))
        f["bath"]       = c2.number_input("Banos",        min_value=0.0, max_value=20.0, step=0.5, value=float(f["bath"]))
        f["acre_lot"]   = c3.number_input("Lote (acres)", min_value=0.0, step=0.01, format="%.2f", value=float(f["acre_lot"]))
        f["house_size"] = c4.number_input("Tamano (sqft)", min_value=0.0, step=10.0, value=float(f["house_size"]))

    with st.expander("Ubicacion", expanded=True):
        c1, c2, c3 = st.columns(3)
        f["city"]     = c1.text_input("Ciudad",        value=f["city"])
        f["state"]    = c2.text_input("Estado",        value=f["state"])
        f["zip_code"] = c3.text_input("Codigo postal", value=f["zip_code"])
        c4, c5 = st.columns(2)
        f["street"]      = c4.text_input("Calle",   value=f["street"])
        f["brokered_by"] = c5.text_input("Agencia", value=f["brokered_by"])

    with st.expander("Informacion adicional", expanded=True):
        c1, c2 = st.columns(2)
        status_idx  = STATUS_OPTIONS.index(f["status"]) if f["status"] in STATUS_OPTIONS else 0
        f["status"] = c1.selectbox("Estado de la propiedad", STATUS_OPTIONS, index=status_idx)
        try:
            parsed_date = date.fromisoformat(f["prev_sold_date"] or "2021-01-01")
        except Exception:
            parsed_date = date(2021, 1, 1)
        f["prev_sold_date"] = c2.date_input("Ultima fecha de venta", value=parsed_date).isoformat()

    st.divider()

    if st.button("Predecir precio", type="primary", use_container_width=True):
        payload = {k: (None if v == "" else v) for k, v in f.items()}
        try:
            resp = requests.post(f"{API_URL}/predict", json=payload, timeout=15)

            if resp.status_code == 200:
                result = resp.json()
                price  = result.get("predicted_price", 0)

                st.markdown(
                    f"""
                    <div style="background:#1b4f7222; border-left:6px solid #1b4f72;
                                padding:1.2rem 1.5rem; border-radius:6px; margin:1rem 0;">
                        <span style="font-size:0.8rem; color:#1b4f72; font-weight:600;
                                     text-transform:uppercase; letter-spacing:0.06em;">
                            Precio estimado
                        </span>
                        <div style="font-size:3rem; font-weight:700; color:#1b4f72; margin-top:0.2rem;">
                            ${price:,.0f}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Modelo",            result.get("model_name", "-"))
                m2.metric("Version",           result.get("model_version", "-"))
                m3.metric("Alias",             result.get("model_alias", "-"))
                m4.metric("Tiempo respuesta",  f"{result.get('response_time_ms', 0):.1f} ms")

            elif resp.status_code == 503:
                st.warning("El modelo aun no esta disponible. Espera a que el DAG complete el primer entrenamiento.")
            elif resp.status_code == 422:
                st.error("Error de validacion en los datos enviados.")
                st.json(resp.json())
            else:
                st.error(f"Error de la API ({resp.status_code})")
                st.json(resp.json())

        except requests.exceptions.ConnectionError:
            st.error(f"No se pudo conectar con la API en `{API_URL}`.")
        except requests.exceptions.Timeout:
            st.error("La API tardo demasiado en responder (timeout 15s).")
        except Exception as exc:
            st.error(f"Error inesperado: {exc}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Historial
# ═══════════════════════════════════════════════════════════════════════════════

def _badge(label, color):
    return (
        f'<span style="background:{color}22; color:{color}; padding:3px 12px; '
        f'border-radius:12px; font-size:0.78rem; font-weight:600; '
        f'letter-spacing:0.04em;">{label}</span>'
    )


def _build_narrative(batch):
    """Returns (training_sentence, promotion_sentence) as plain strings."""
    should_train  = batch.get("should_train")
    reasons       = batch.get("training_reasons") or []
    model_version = batch.get("model_version")
    model_promoted = batch.get("model_promoted")
    promo_reason  = batch.get("promotion_reason") or ""

    if isinstance(reasons, list):
        reasons_text = "; ".join(reasons) if reasons else "criterios de reentrenamiento cumplidos"
    else:
        reasons_text = str(reasons)

    if should_train is True:
        train_sentence = f"Entrenó porque: {reasons_text}."
    elif should_train is False:
        train_sentence = f"No entrenó. {reasons_text}."
    else:
        train_sentence = "Estado de entrenamiento no registrado."

    if model_version is None:
        promo_sentence = None
    elif model_promoted is True:
        promo_sentence = f"Promovido a producción. {promo_reason}"
    elif model_promoted is False:
        promo_sentence = f"No promovido. {promo_reason}"
    else:
        promo_sentence = None

    return train_sentence, promo_sentence


with tab_history:
    col_r, _ = st.columns([1, 6])
    with col_r:
        refresh = st.button("Actualizar", key="refresh_history")

    batches = get_batch_history()

    if batches is None:
        st.error(
            "No se pudo obtener el historial desde la API. "
            "Verifica que el servicio este corriendo y que el DAG haya creado la tabla batch_audit."
        )
        st.stop()

    if len(batches) == 0:
        st.info("Aun no hay lotes registrados. El DAG comenzara a procesar lotes segun su programacion.")
        st.stop()

    # ── Summary metrics ───────────────────────────────────────────────────────

    total    = len(batches)
    trained  = sum(1 for b in batches if b.get("should_train") is True)
    promoted = sum(1 for b in batches if b.get("model_promoted") is True)
    success  = sum(1 for b in batches if b.get("execution_status") == "success")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Lotes procesados",    total)
    m2.metric("Con entrenamiento",   trained)
    m3.metric("Modelos promovidos",  promoted)
    m4.metric("Tasa de promocion",   f"{promoted/trained:.0%}" if trained else "—")

    st.divider()

    # ── Per-batch cards (most recent first) ───────────────────────────────────

    for batch in reversed(batches):
        batch_id   = batch.get("batch_id", "?")
        status     = batch.get("execution_status", "unknown")
        fetched_at = (batch.get("fetched_at") or "")[:10]
        records    = batch.get("records_received") or 0
        should_train   = batch.get("should_train")
        model_version  = batch.get("model_version")
        model_promoted = batch.get("model_promoted")
        candidate_mae  = batch.get("candidate_mae")
        candidate_rmse = batch.get("candidate_rmse")
        production_mae = batch.get("production_mae")
        production_rmse = batch.get("production_rmse")
        model_run_id   = batch.get("model_run_id")
        drift_detected = batch.get("drift_detected")
        drift_details  = batch.get("drift_details") or {}
        new_cats       = batch.get("new_categories_detected") or {}

        train_sentence, promo_sentence = _build_narrative(batch)

        # Expand the most recent batch by default
        is_latest = batch_id == batches[-1].get("batch_id")
        header = f"Lote {batch_id}  ·  {fetched_at}  ·  {records:,} registros"

        with st.expander(header, expanded=is_latest):

            # Badge row
            STATUS_COLORS  = {"success": "#27ae60", "failed": "#c0392b", "running": "#e67e22"}
            status_color   = STATUS_COLORS.get(status, "#7f8c8d")
            train_color    = "#2980b9" if should_train else "#95a5a6"
            promo_color    = "#27ae60" if model_promoted else ("#c0392b" if model_promoted is False else "#95a5a6")

            badges = [_badge(status.upper(), status_color)]
            if should_train is True:
                badges.append(_badge("ENTRENÓ", "#2980b9"))
            elif should_train is False:
                badges.append(_badge("NO ENTRENÓ", "#95a5a6"))
            if model_promoted is True:
                badges.append(_badge("PROMOVIDO", "#27ae60"))
            elif model_promoted is False:
                badges.append(_badge("RECHAZADO", "#c0392b"))

            st.markdown("&nbsp;&nbsp;".join(badges), unsafe_allow_html=True)
            st.write("")

            # Narrative
            st.write(f"**Entrenamiento:** {train_sentence}")
            if promo_sentence:
                st.write(f"**Promocion:** {promo_sentence}")

            # Drift / category signals
            signals = []
            if drift_detected:
                drifted_cols = list(drift_details.keys()) if isinstance(drift_details, dict) else []
                signals.append(f"Drift en: {', '.join(drifted_cols) if drifted_cols else 'columnas numericas'}")
            if new_cats:
                cat_cols = list(new_cats.keys()) if isinstance(new_cats, dict) else []
                if cat_cols:
                    signals.append(f"Nuevas categorias en: {', '.join(cat_cols)}")
            if signals:
                st.caption("Senales: " + "  |  ".join(signals))

            # Performance metrics (only if a candidate was trained)
            if candidate_mae is not None:
                st.divider()
                st.subheader("Desempeno del candidato")
                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("MAE candidato",  f"{candidate_mae:,.0f}")
                mc2.metric("RMSE candidato", f"{candidate_rmse:,.0f}" if candidate_rmse else "—")

                if production_mae is not None:
                    mae_delta  = (candidate_mae - production_mae) / production_mae * 100
                    rmse_delta = (
                        (candidate_rmse - production_rmse) / production_rmse * 100
                        if candidate_rmse and production_rmse else None
                    )
                    mc3.metric(
                        "MAE produccion", f"{production_mae:,.0f}",
                        f"{mae_delta:+.1f}% vs produccion",
                        delta_color="inverse",
                    )
                    mc4.metric(
                        "RMSE produccion",
                        f"{production_rmse:,.0f}" if production_rmse else "—",
                        f"{rmse_delta:+.1f}% vs produccion" if rmse_delta is not None else None,
                        delta_color="inverse",
                    )
                else:
                    mc3.metric("MAE produccion",  "—", "Linea base — sin modelo previo")
                    mc4.metric("RMSE produccion", "—")

            # MLflow identifiers
            if model_run_id or model_version:
                st.divider()
                st.subheader("Identificadores MLflow")
                idc1, idc2 = st.columns(2)
                if model_run_id:
                    idc1.code(f"Run ID:  {model_run_id}")
                if model_version:
                    idc2.code(f"Version: {model_version}")
