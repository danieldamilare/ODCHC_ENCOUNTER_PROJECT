''' Parse SQL Params'''

from dataclasses import dataclass
from typing import Any, List, Optional, Dict, Union, Type, Tuple
from exceptions import ValidationError
@dataclass
class AndFilter:
    model: Type
    col: str
    op: str
    value: str

@dataclass
class OrFilter(AndFilter):
    pass

@dataclass
class GroupBy:
    model: Any
    col: str

@dataclass
class OrderBy:
    model: Any
    col: str
    order: str

@dataclass
class Params:
    and_filter: List[AndFilter]
    or_filter: List[OrFilter]
    group_by: List[GroupBy]
    order_by: List[OrderBy]
    limit: int

class FilterParser:
    ALLOWED_OPERATORS = {'=', '>', '<', '>=', '<=', '!=', 'LIKE', 'IN'}

    @classmethod
    def make_params(cls, and_filter: Optional[List[AndFilter]] = None,
                    or_filter: Optional[List[AndFilter]] = None,
                    group_by: Optional[List[AndFilter]] = None,
                    order_by: Optional[List[AndFilter]] = None,
                    limit: int = 0) -> Params:
        if and_filter is None:
            and_filter = []
        if or_filter is None:
            or_filter = []
        if group_by is None:
            group_by = []
        if order_by is None:
            Order_by = []
        return Params(
            and_filter, or_filter,
            group_by, order_by
        )

    @classmethod
    def parse_params(cls, params: Params, model_map: Dict):
        and_filter = cls.parse_filters(params.and_filter, model_map)
        or_filter = cls.parse_filters(paramas.or_filter, model_map)
        group_by = cls.parse_groupby(params.group_by, model_map)
        order_by = cls.parse_orderby(params.order_by, model_map)
        return {
            'and_filter': and_filter,
            'or_filter': or_filter,
            'group_by': group_by,
            'order_by': order_by
        }

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
