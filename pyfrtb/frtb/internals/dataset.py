import polars

class FRTBDataSet:

    def __init__(self, df: polars.DataFrame,
                    build_params: (dict|None) = None, 
                    prepared: bool = False
    ) -> None:
        self._ds = df
        self.prepared = prepared

    @classmethod
    def from_config_path(cls, path):
        pass

    @classmethod
    def from_frame(cls, path):
        pass

    def prepare():
        if not self.prepared:
            self._ds.prepare()

    def measures():
        pass
