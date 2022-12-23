import json
from ..rust_module.ultima_pyengine import AggregationRequestWrapper

class AggRequest:
    """
    This is python based twin of AggregationRequest struct
    Examples
    --------
    Constructing a FRTB AggRequest from a dictionary:

    >>> r = dict(
    ... measures=[("SBM Charge", "scalar"), ("FX Delta Sensitivity", "sum")], 
    ... groupby=["RiskCategory"], 
    ... totals=True,
    ... calc_params={"jurisdiction": "BCBS"}
    ... )
    >>> ar = frtb.AggRequest(sr)

    """
    def __init__(
        self, 
        data: (
            dict
            |str
            ) ,
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
