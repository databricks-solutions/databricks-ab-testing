from __future__ import annotations

import datetime
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import mlflow
import mlflow.pyfunc
import mlflow.sklearn
import numpy as np
import pandas as pd

from src.AssignmentService import AssignmentService
from src.utils.lakebase import LakebaseClient


def _hash01(*parts: str) -> float:
    s = ":".join(str(p) for p in parts)
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()
    return int(h, 16) / float(2**256)


def _is_late_hour(hour: int) -> bool:
    return (hour >= 20) or (hour <= 2)


class CTRPyFunc(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        cfg = context.model_config or {}

        self._model = mlflow.sklearn.load_model(context.artifacts["ctr_model"])

        self.db = LakebaseClient()
        self.ad_features_table_path = cfg.get("ad_features_table_path")
        self.user_features_table_path = cfg.get("user_features_table_path")
        self.experiments_table_path = cfg.get("experiments_table_path")
        self.default_flags = cfg.get("default_flags", {})

        for k in ("ad_features_table_path", "user_features_table_path", "experiments_table_path"):
            if not getattr(self, k):
                raise ValueError(f"model_config.{k} is required")

        self.assigner = AssignmentService(
            db=self.db,
            experiments_table_path=self.experiments_table_path,
            default_flags=self.default_flags,
        )

    def _assign(self, user_id: Any) -> Tuple[str, str, Dict[str, Any]]:
        return self.assigner.assign_one(str(user_id))

    def _apply_flags_np(self, df: pd.DataFrame, probs: np.ndarray, flags: Dict[str, Any], user_id: str) -> pd.DataFrame:
        """
        Adds:
          - predicted_ctr (raw prob)
          - ranked_ctr (after temperature / interaction / uplift / epsilon / floor / cap)
          - rank_score  (ranked_ctr * _ad_quality_for_rank)
        """
        temperature = float(flags.get("temperature", 1.0))
        ctr_floor = float(flags.get("ctr_floor", 0.0))
        ctr_cap = float(flags.get("ctr_cap", 1.0))
        use_inter = bool(flags.get("use_interaction_boost", False))
        inter_mul = float(flags.get("interaction_boost_strength", 1.05))
        epsilon = float(flags.get("epsilon_explore", 0.0))
        uplift_map = flags.get("device_uplift", {}) or {}

        p = probs.astype(float)

        if temperature != 1.0:
            eps = 1e-12
            p = np.clip(p, eps, 1 - eps)
            logit = np.log(p / (1 - p))
            p = 1.0 / (1.0 + np.exp(-logit / temperature))

        device = df["device"].iloc[0]
        hour = int(df["hour"].iloc[0])
        if use_inter and device == "mobile" and _is_late_hour(hour):
            p = np.minimum(p * inter_mul, 1.0)

        uplift = float(uplift_map.get(device, 1.0))
        if uplift != 1.0:
            p = np.minimum(p * uplift, 1.0)

        if epsilon > 0.0 and "ad_id" in df.columns:
            bumped = []
            for ad_id, pi in zip(df["ad_id"].tolist(), p.tolist()):
                r = _hash01(str(user_id), str(ad_id), "epsilon")
                bumped.append(max(pi, 0.12) if r < epsilon else pi)
            p = np.array(bumped, dtype=float)

        p = np.clip(p, ctr_floor, ctr_cap)

        rank_score = p * df["_ad_quality_for_rank"].astype(float).values

        out = df.copy()
        out["predicted_ctr"] = probs
        out["ranked_ctr"] = p
        out["rank_score"] = rank_score
        return out

    def _fetch_user_features(self, user_id: str) -> Dict[str, Any]:
        sql = f"""
            SELECT age_group, historical_ctr_30d, sessions_30d, impressions_30d, clicks_30d, days_since_signup
            FROM {self.user_features_table_path}
            WHERE user_id = %s
            LIMIT 1
        """
        rows = self.db.fetch(sql, (user_id,))
        return rows[0] if rows else {}

    def _fetch_ad_features(self, device: str, region: str) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT ad_id, category, _ad_quality_for_rank
            FROM {self.ad_features_table_path}
            WHERE active_from <= CURRENT_DATE
              AND (active_to IS NULL OR active_to > CURRENT_DATE)
            LIMIT 500
        """
        return self.db.fetch(sql, ())

    def _now_hour(self) -> int:
        return int(datetime.datetime.utcnow().hour)

    def predict(self, context, model_input: pd.DataFrame, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        req = model_input.to_dict(orient="records")[0]

        for key in ("user_id", "device", "region"):
            if key not in req:
                raise ValueError(f"Request must include '{key}'.")

        user_id = str(req["user_id"])
        device = str(req["device"])
        region = str(req["region"])
        k = int(req.get("k", 5))

        experiment_id, variant, flags = self._assign(user_id)

        user_feats = self._fetch_user_features(user_id)
        ads = self._fetch_ad_features(device=device, region=region)

        rows_df = self._build_rows_pd(req, user_feats, ads, flags)
        if rows_df.empty:
            return {
                "user_id": user_id,
                "experiment_id": experiment_id,
                "variant": variant,
                "flags": flags,
                "top_k": [],
                "features_found": bool(user_feats),
            }

        X = rows_df[
            [
                "hour",
                "device",
                "region",
                "category",
                "ad_quality",
                "age_group",
                "historical_ctr_30d",
                "sessions_30d",
                "impressions_30d",
                "clicks_30d",
                "days_since_signup",
            ]
        ]

        probs = self._model.predict_proba(X)[:, 1]

        ranked = self._apply_flags_np(rows_df, probs, flags, user_id)
        topk = (
            ranked.sort_values("rank_score", ascending=False)
            .loc[:, ["ad_id", "rank_score", "ranked_ctr", "predicted_ctr", "_ad_quality_for_rank"]]
            .rename(columns={"_ad_quality_for_rank": "ad_quality"})
            .head(k)
        )

        return {
            "user_id": user_id,
            "experiment_id": experiment_id,
            "variant": variant,
            "flags": flags,
            "top_k": topk.to_dict(orient="records"),
            "features_found": bool(user_feats),
        }

    def _build_rows_pd(self, req, user_feats, ads, flags) -> pd.DataFrame:
        device = str(req["device"])
        region = str(req["region"])

        # User features from the fetched user data
        age_group = user_feats.get("age_group", "25-34")  # Default to most common age group
        historical_ctr_30d = float(user_feats.get("historical_ctr_30d", 0.0))
        sessions_30d = int(user_feats.get("sessions_30d", 0))
        impressions_30d = int(user_feats.get("impressions_30d", 0))
        clicks_30d = int(user_feats.get("clicks_30d", 0))
        days_since_signup = int(user_feats.get("days_since_signup", 365))  # Default to 1 year

        boost_factor = float(flags.get("boost_factor", 1.0))

        now_hour = self._now_hour()
        rows = []
        for a in ads:
            aq = float(a.get("_ad_quality_for_rank", 0.5))
            if boost_factor != 1.0:
                aq *= boost_factor
            rows.append(
                {
                    "ad_id": a.get("ad_id"),
                    "hour": now_hour,
                    "device": device,
                    "region": region,
                    "category": a.get("category", "other"),
                    "ad_quality": aq,
                    "age_group": age_group,
                    "historical_ctr_30d": historical_ctr_30d,
                    "sessions_30d": sessions_30d,
                    "impressions_30d": impressions_30d,
                    "clicks_30d": clicks_30d,
                    "days_since_signup": days_since_signup,
                    "_ad_quality_for_rank": aq,
                }
            )

        return pd.DataFrame(
            rows,
            columns=[
                "ad_id",
                "hour",
                "device",
                "region",
                "category",
                "ad_quality",
                "age_group",
                "historical_ctr_30d",
                "sessions_30d",
                "impressions_30d",
                "clicks_30d",
                "days_since_signup",
                "_ad_quality_for_rank",
            ],
        )


mlflow.models.set_model(model=CTRPyFunc())
