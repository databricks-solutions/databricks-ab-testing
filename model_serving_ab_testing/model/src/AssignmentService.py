from __future__ import annotations

import hashlib
from typing import Any, Dict, Tuple


def _normalized_hash(*parts: str) -> float:
    key = ":".join(str(p) for p in parts)
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()

    return int(h, 16) / float(2**256)


def _choose_variant(experiment_id: str, unit_id: str, allocations: Dict[str, float]) -> str:
    r = _normalized_hash(experiment_id, unit_id)
    cum = 0.0
    last = None
    for v, p in allocations.items():
        cum += float(p)
        last = v
        if r < cum:
            return v
    return last


def _merge_override_flags(default: dict, override: dict | None) -> dict:
    if not override:
        return default

    out = default.copy()
    for k, v in override.items():
        # Skip None so we don't overwrite defaults with nulls
        if v is None:
            continue

        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge_override_flags(out[k], v)
        else:
            out[k] = v
    return out


class AssignmentService:
    """
    Deterministically assigns a unit to a variant and returns that variant's flags.
    """

    def __init__(
        self,
        db,
        experiments_table_path: str,
        default_flags: Dict[str, Any],
    ):
        self.db = db
        self.experiments_table_path = experiments_table_path
        self.default_flags = default_flags or {}

        experiment_data = self._fetch_experiment_data()
        self.experiment_id = experiment_data.get("experiment_id", None)
        self.experiment_flag_overrides = {
            "control": experiment_data.get("control_config", None),
            "treatment": experiment_data.get("treatment_config", None),
        }

        treatment_allocation = float(experiment_data.get("treatment_allocation", 0))
        self.allocations = {"control": 1 - treatment_allocation, "treatment": treatment_allocation}

        self.flags_by_variant = _merge_override_flags(
            default={"treatment": self.default_flags, "control": self.default_flags},
            override=self.experiment_flag_overrides,
        )

    def _fetch_experiment_data(self) -> Dict[str, Any]:
        sql = f"""
            SELECT experiment_id, treatment_allocation, control_config, treatment_config
            FROM {self.experiments_table_path}
            WHERE status = 'Published'
                AND current_date BETWEEN start_date AND end_date
            LIMIT 1
        """
        rows = self.db.fetch(sql)
        return rows[0] if rows else {}

    def assign_one(self, unit_id: Any) -> Tuple[str | None, str, Dict[str, Any]]:
        if not self.experiment_id:
            return None, "control", self.default_flags

        variant = _choose_variant(self.experiment_id, str(unit_id), self.allocations)
        return self.experiment_id, variant, self.flags_by_variant[variant]
