from __future__ import annotations

import json
from typing import Any, TypeVar

from ..rust_module.ultibi_engine import FilterWrapper

# Create a generic variable that can be 'Parent', or any subclass.
AnyFilter = TypeVar("AnyFilter", bound="Filter")


class Filter:
    """
    This is Filter to be applied on the data with your request

    Examples
    --------
    Constructing a Filter from a dictionary:

    >>> from ultibi.internals.filters import Filter
    >>> filter = dict(op="Eq", field="Desk", value="FXOptions")
    >>> f = Filter(filter)
    >>> filter = dict(op="In", field="Desk", value=["FXOptions", "Rates"])
    >>> f = Filter(filter)

    """

    inner: FilterWrapper

    def __init__(
        self,
        data: "dict[Any, Any] | str",
    ) -> None:
        if isinstance(data, dict):
            json_str = json.dumps(data)
            self.inner = FilterWrapper.from_str(json_str)

        elif isinstance(data, str):
            self.inner = FilterWrapper.from_str(data)

        else:
            raise ValueError(
                f"Filter constructor \
                    called with unsupported type; got {type(data)}"
            )

    def __str__(self) -> str:
        return self.inner.as_str()

    def __repr__(self) -> str:
        return self.__str__()


class EqFilter(Filter):
    """Field Equilas Value

    Examples
    --------
    Constructing a EqFilter:

    >>> import ultibi as ul
    >>> f = ul.EqFilter(field="Desk", value="FXOptions")
    """

    op = "Eq"

    def __init__(self, field: str, value: str) -> None:
        """

        Args:
            field (str): Column
            value (str): Value
        """
        data = dict(op=self.op, field=field, value=value)
        super().__init__(data)


class NeqFilter(Filter):
    """Field Not Equals Value

    Examples
    --------
    Constructing a NeqFilter:

    >>> import ultibi as ul
    >>> f = ul.NeqFilter(field="Desk", value="FXOptions")
    """

    op = "Neq"

    def __init__(self, field: str, value: str) -> None:
        """

        Args:
            field (str): Column
            value (str): Value
        """
        data = dict(op=self.op, field=field, value=value)
        super().__init__(data)


class InFilter(Filter):
    """Field Value is one of list/series/vec

    Examples
    --------
    Constructing a InFilter:

    >>> import ultibi as ul
    >>> f = ul.InFilter(field="Sex", value=["Male", "Female"])
    """

    op = "In"

    def __init__(self, field: str, value: list[str]) -> None:
        """

        Args:
            field (str): Column
            value (list[str]): Values
        """
        data = dict(op=self.op, field=field, value=value)
        super().__init__(data)


class NotInFilter(Filter):
    """Field Value is not one of list/series/vec

    Examples
    --------
    Constructing a NotInFilter:

    >>> import ultibi as ul
    >>> f = ul.NotInFilter(field="Sex", value=["Male", "Female"])
    """

    op = "NotIn"

    def __init__(self, field: str, value: list[str]) -> None:
        """

        Args:
            field (str): Column
            value (list[str]): Values
        """
        data = dict(op=self.op, field=field, value=value)
        super().__init__(data)
