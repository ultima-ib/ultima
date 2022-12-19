import json

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
            self._ar = frtb.frtb_pyengine.AggregationRequestWrapper.from_str(jason_str)

        elif isinstance(data, str):
            self._ar = frtb.frtb_pyengine.AggregationRequestWrapper.from_str(data)
        
        else:
            raise ValueError(
                f"AggRequest constructor called with unsupported type; got {type(data)}"
            )

        """
        self.name = None
        self.measures = []
        self.groupby = []

        self.filters = []
        self.overrides = []
        self.add_row = []
        self.calc_params = []

        self.hide_zeros = False
        self.totals = False
        """

