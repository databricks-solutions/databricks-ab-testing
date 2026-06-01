import math
from typing import Optional, Tuple

from statsmodels.stats.power import NormalIndPower, TTestIndPower
from statsmodels.stats.proportion import proportion_effectsize


def compute_sample_sizes(
    kpi_type: str,
    baseline: Optional[float],
    mde: float,
    power: float,
    alpha: float,
    treatment_allocation: float,
) -> Tuple[int, int]:
    """
    Returns (n_control, n_treatment).
    - kpi_type: continuous or proportion.
    - baseline: p0 for proportion; sigma for continuous. If None, uses p0=0.5 or sigma=60.0 as in your code.
    - mde: absolute difference (pp for CTR, units for continuous).
    - power, alpha in (0,1)
    - treatment_allocation: r in (0,1) = fraction of traffic to treatment.
    """
    r = max(1e-6, min(1 - 1e-6, treatment_allocation))
    ratio = r / (1 - r)

    if kpi_type == "continuous":
        sigma = baseline if baseline is not None else 60.0
        effect_size = mde / sigma
        solver = TTestIndPower()
    else:
        p0 = baseline if baseline is not None else 0.5
        p1 = p0 + mde
        if not (0 < p1 < 1):
            raise ValueError("p0 + mde must be in (0,1) for proportion power calc.")
        effect_size = proportion_effectsize(p1, p0)
        solver = NormalIndPower()

    n_control = math.ceil(
        solver.solve_power(effect_size=effect_size, power=power, alpha=alpha, ratio=ratio, alternative="two-sided")
    )
    n_treatment = math.ceil(n_control * ratio)
    return n_control, n_treatment
