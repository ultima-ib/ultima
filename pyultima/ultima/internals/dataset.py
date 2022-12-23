import polars as pl
from ..rust_module.ultima_pyengine import OtherError, DataSetWrapper
from typing import TypeVar, Type
# Create a generic variable that can be 'Parent', or any subclass.
T = TypeVar('T', bound='DataSet')

class DataSet:
    """
    Main DataSet class

    Holds data, calls prepare on Data, validates.

    Raises:
        OtherError: When trying to prepare a prepared dataset

    Returns:
        _type_: _description_
    """

    ds: DataSetWrapper
    prepared: bool 

    def __init__(self, 
                ds: DataSetWrapper,
                prepared: bool = False
    ) -> None:

        self._ds = ds
        self.prepared = prepared

    @classmethod
    def from_config_path(cls: Type[T], path: str) -> T:
        """
        Reads path to <config>.toml
        Converts into DataSourceConfig
        Builds DataSet from DataSourceConfig

        Args:
            path (str): path to <config>.toml

        Returns:
            T: Self
        """
        return cls(DataSetWrapper.from_config_path(path), False)

    @classmethod
    def from_frame(cls: Type[T], 
            df: pl.DataFrame,
            measures: list[str] = None,
            build_params: (dict|None) = None, 
            prepared = False
        ) -> T:
        """
        Build DataSet directly from df

        Args:
            cls (Type[T]): _description_
            df (polars.DataFrame): _description_
            measures (list[str], optional): Used as a constrained on measures.
                Defaults to all numeric columns in the dataframe.
            build_params (dict | None, optional): Params to be used in prepare. Defaults
             to None.
            prepared (bool, optional): Was your DataFrame already prepared? Defaults 
            to False.

        Returns:
            T: Self
        """
        return cls(DataSetWrapper.from_frame(df, measures, build_params), prepared)


    def prepare(self):
        if not self.prepared:
            self._ds.prepare()
            self.prepared = True
        else:
            raise OtherError("Calling prepare on an already prepared dataset")
    
    def validate(self):
        """TODO: validate when FRTBDataSet validate is ready
        """
        pass

    def measures(self) -> dict[str, str|None]:
        """Measures availiable on the DataSet

        Returns:
            dict[str, str|None]: {measureName: "aggtype restriction(if any, otherwise 
            None)"}
        """
        return self._ds.measures()
    
    def frame(self) -> pl.DataFrame:
        vec_srs = self._ds.frame()
        return pl.DataFrame(vec_srs)


class FRTBDataSet(DataSet):
    """FRTB flavour of DataSet
    """

    @classmethod
    def from_config_path(cls: Type[T], path: str) -> T:
        return cls(DataSetWrapper.frtb_from_config_path(path), False)

    @classmethod
    def from_frame(cls: Type[T], 
            df: pl.DataFrame,
            measures: list[str] = None,
            build_params: (dict|None) = None, 
            prepared = False
        ) -> T:
        return cls(DataSetWrapper.frtb_from_frame(df, measures, build_params), prepared)

    
