from __future__ import annotations

import json
from typing import Any

from ..rust_module.ultibi_engine import (
    AggregationRequestWrapper,
    ComputeRequestWrapper,
    agg_ops,
)


def aggregation_ops() -> list[str]:
    """
    Returns:
        list[str]: List of supported aggregation operations.
        This is to be used in AggRequest measures field.
    """
    return agg_ops()


class AggRequest:
    """
    This is python binding of AggregationRequest struct. This is the most basic kind of
    request. It is executed within .groupby().apply() context. Check aggregation_ops
    for availiable operations for measures.

    Examples
    --------
    Constructing a FRTB AggRequest from a dictionary:

    >>> import ultibi as ul
    >>> request_as_duct = dict(
    ...     measures=[("SBM Charge", "scalar"), ("FX Delta Sensitivity", "sum")],
    ...     groupby=["RiskCategory"],
    ...     totals=True,
    ...     calc_params={"jurisdiction": "BCBS"},
    ... )
    >>> ar = ul.AggRequest(request_as_duct)

    """

    def __init__(
        self,
        data: "dict[Any, Any] | str",
    ) -> None:
        if isinstance(data, dict):
            jason_str = json.dumps(data)
            self._ar = AggregationRequestWrapper.from_str(jason_str)

        elif isinstance(data, str):
            self._ar = AggregationRequestWrapper.from_str(data)

        else:
            raise ValueError(
                f"AggRequest constructor called with unsupported type; got {type(data)}"
            )

    def __str__(self) -> str:
        return self._ar.as_str()

    def __repr__(self) -> str:
        return self.__str__()


class ComputeRequest:
    """
    This is python binding of AggregationRequest struct. This is the most basic kind of
    request. It is executed within .groupby().apply() context. Check aggregation_ops
    for availiable operations for measures.

    Examples
    --------
    Constructing a FRTB AggRequest from a dictionary:

    >>> import ultibi as ul
    >>> request_as_dict = dict(
    ...     measures=[("SBM Charge", "scalar"), ("FX Delta Sensitivity", "sum")],
    ...     groupby=["RiskCategory"],
    ...     totals=True,
    ...     calc_params={"jurisdiction": "BCBS"},
    ... )
    >>> ar = ul.ComputeRequest(request_as_dict)

    """

    cr: ComputeRequestWrapper

    def __init__(
        self,
        data: "dict[Any, Any] | str",
    ) -> None:
        if isinstance(data, dict):
            json_str = json.dumps(data)
            self._ar = ComputeRequestWrapper.from_str(json_str)

        elif isinstance(data, str):
            self._ar = ComputeRequestWrapper.from_str(data)

        else:
            raise ValueError(
                f"ComputeRequest constructor \
                    called with unsupported type; got {type(data)}"
            )

    def __str__(self) -> str:
        return self._ar.as_str()

    def __repr__(self) -> str:
        return self.__str__()
