''' Parse SQL Params'''

from dataclasses import dataclass, field
from typing import Any, List, Optional, Dict, Union, Type, Tuple
from app.config import Config
from exceptions import ValidationError
@dataclass
class AndFilter:
    model: Type
    col: str
    op: str
    value: Any

@dataclass
class OrFilter(AndFilter):
    pass

@dataclass
class GroupBy:
    model: Type
    col: str

@dataclass
class OrderBy:
    model: Type
    col: str
    order: str = 'ASC'

@dataclass
class Params:
    and_filter: List[AndFilter]  = field(default_factory=list)
    or_filter: List[OrFilter]  = field(default_factory=list)
    group_by: List[GroupBy]  = field(default_factory=list)
    order_by: List[OrderBy]  = field(default_factory=list)
    limit: int = field(default=0)
    offset: int = field(default=0)

    def where(self, model: Type, col: str, op: str, value: Any):
        self.and_filter.append(AndFilter(model, col, op, value))
        return self

    def or_where(self, model: Type, col: str, value: Any):
        self.or_filter.append(OrFilter(model, col, op, value))
        return self

    def group(self, model:Type, col: str):
        self.group_by.append(GroupBy(model, col))
        return self

    def sort(self, model:Type, col: str, order: str = 'ASC'):
        self.order_by.append(OrderBy(model, col, order))
        return self

    def paginate(self, limit: int = 0,
                 offset: int = 0):
        self.limit = limit
        self.offset = offset
        return self

class FilterParser:
    ALLOWED_OPERATORS = {'=', '>', '<', '>=', '<=', '!=', 'LIKE', 'IN'}

    @classmethod
    def parse_params(cls, params: Params, model_map: Dict):
        result = {}
        if params.and_filter:
            result['and_filter'] = cls.parse_filters(params.and_filter, model_map)
        if params.or_filter:
            result[ 'or_filter' ] = cls.parse_filters(params.or_filter, model_map)
        if params.group_by:
            result[ 'group_by' ] = cls.parse_groupby(params.group_by, model_map)
        if params.order_by:
           result [ 'order_by' ] = cls.parse_orderby(params.order_by, model_map)
        if params.limit > 0:
            result['limit'] = params.limit
        if params.offset > 0:
            result['offset'] = params.offset
        return result

    @classmethod
    def parse_filters(cls, filters: List[Union[AndFilter, OrFilter]],
                      model_map: Dict) -> List[Tuple]:
        result = []
        for fil in filters:
            model = fil.model
            op = fil.op
            col = fil.col
            value = fil.value

            if not model.validate_col(col):
                raise ValidationError(f"Column {col} not in table {model.get_name()}")
            if not op in cls.ALLOWED_OPERATORS:
                raise ValidationError(f"Operator {op} not allowed")
            if not model in model_map:
                raise ValidationError("Model not in Model map")

            result.append((f'{model_map[model]}.{col}', value, op ))
        return result

    @classmethod
    def parse_groupby(cls, filters: List[GroupBy],
                      model_map: Dict):
        result = []
        for fil in filters:
            model = fil.model
            col = fil.col
            if not model.validate_col(col):
                raise ValidationError(f"Column {col} not in table {model.get_name()}")
            if not model in model_map:
                raise ValidationError("Model not in Model map")

            result.append(f'{model_map[model]}.{col}')
        return result

    @classmethod
    def parse_orderby(cls, filters: List[OrderBy],
                      model_map: Dict):
        result = []
        for fil in filters:
            model = fil.model
            col = fil.col
            order = fil.order.upper()

            if not model.validate_col(col):
                raise ValidationError(f"Column {col} not in table {model.get_name()}")
            if not model in model_map:
                raise ValidationError("Model not in Model map")
            if not order in {'ASC', 'DESC'}:
                raise ValidationError("Invalid Order Format")
            result.append((f'{model_map[model]}.{col}', order))
        return result
