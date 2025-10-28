''' Parse SQL Params'''

from dataclasses import dataclass, field, replace
from typing import Any, List, Optional, Dict, Union, Type, Tuple
from app.config import Config
from app.exceptions import QueryParameterError
@dataclass
class Filter:
    model: Optional[ Type ]
    col: str
    op: str
    value: Any

@dataclass
class GroupBy:
    model: Optional[Type]
    col: str

@dataclass
class OrderBy:
    model: Optional[Type]
    col: str
    order: str = 'ASC'

@dataclass(frozen=True)
class Params:
    _and_filter: tuple = field(default_factory=tuple)
    _or_filter:tuple  = field(default_factory=tuple)
    _group_by: tuple  = field(default_factory=tuple)
    _order_by: tuple  = field(default_factory=tuple)
    _limit: int = 0
    _offset: int = 0

    def where(self, model: Type, col: str, op: str, value: Any) -> 'Params':
        return replace(self, _and_filter = self._and_filter + (Filter(model, col, op, value),))

    def or_where(self, model: Type, col: str, op: str, value: Any) -> 'Params':
        return replace(self, _or_filter = self._or_filter + (Filter(model, col, op, value),))

    def group(self, model:Type, col: str) -> 'Params':
        return replace(self, _group_by = self._group_by + (GroupBy(model, col),))

    def sort(self, model:Type, col: str, order: str = 'ASC') -> 'Params':
        return replace(self, _order_by = self._order_by + (OrderBy(model, col, order), ))

    def set_limit(self, limit: int = 0) -> 'Params':
        return replace(self, _limit = limit)

    def set_offset(self, offset: int = 0) -> 'Params':
        return replace(self,  _offset = offset)

    @property
    def and_filter(self) -> List[Filter]:
        return list(self._and_filter)

    @property
    def or_filter(self) -> List[Filter]:
        return list(self._or_filter)

    @property
    def group_by(self) -> List[GroupBy]:
        return list(self._group_by)

    @property
    def order_by(self) -> List[OrderBy]:
        return list(self._order_by)

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def offset(self) -> int:
        return self._offset

class FilterParser:
    ALLOWED_OPERATORS = {'=', '>', '<', '>=', '<=', '!=', 'LIKE', 'IN'}

    @classmethod
    def parse_params(cls, params: Params, model_map: Dict):
        result = {}
        if and_filter := params.and_filter:
            result['and_filter'] = cls.parse_filters(and_filter, model_map)
        if or_filter := params.or_filter:
            result[ 'or_filter' ] = cls.parse_filters(or_filter, model_map)
        if group_by := params.group_by:
            result[ 'group_by' ] = cls.parse_groupby(group_by, model_map)
        if order_by := params.order_by:
           result [ 'order_by' ] = cls.parse_orderby(order_by, model_map)
        if params.limit > 0:
            result['limit'] = params.limit
        if params.offset > 0:
            result['offset'] = params.offset
        return result

    @classmethod
    def parse_filters(cls, filters: List[Filter],
                      model_map: Dict) -> List[Tuple]:
        result = []
        save = {}
        for fil in filters:
            model = fil.model
            op = fil.op
            col = fil.col
            value = fil.value
            if model:
                if not model.validate_col(col):
                    raise QueryParameterError(f"Column {col} not in table {model.get_name()}")
                if not model in model_map:
                    print(fil)
                    raise QueryParameterError(f"Model {model} not in Model map")
                col = f'{model_map[model]}.{col}'

            if not op in cls.ALLOWED_OPERATORS:
                raise QueryParameterError(f"Operator {op} not allowed")
            if (col, op) in save:
                save[(col, op)] = (col, value, op)
            else:
                save[(col, op)] = len(result)
                result.append((col, value, op))
        return result

    @classmethod
    def parse_groupby(cls, filters: List[GroupBy],
                      model_map: Dict):
        result = []
        save = {}

        for fil in filters:
            model = fil.model
            col = fil.col
            if model:
                if not model.validate_col(col):
                    raise QueryParameterError(f"Column {col} not in table {model.get_name()}")
                if not model in model_map:
                    print(fil)
                    raise QueryParameterError(f"Model {model} not in Model map")
                col = (f'{model_map[model]}.{col}')
            if col not in save:
                save[col] = len(result)
                result.append(col)
        return result

    @classmethod
    def parse_orderby(cls, filters: List[OrderBy],
                      model_map: Dict):
        result = []
        save = {}

        for fil in filters:
            model = fil.model
            col = fil.col
            order = fil.order.upper()
            if model:
                if not model.validate_col(col):
                    raise QueryParameterError(f"Column {col} not in table {model.get_name()}")
                if not model in model_map:
                    print(fil)
                    raise QueryParameterError(f"Model {model} not in Model map")
                col = f'{model_map[model]}.{col}'

            if not order in {'ASC', 'DESC'}:
                raise QueryParameterError("Invalid Order Format")

            if col not in save:
                save[col] = len(result)
                result.append((col, order))
            else:
                result[save[col]] = (col, order)
        return result
