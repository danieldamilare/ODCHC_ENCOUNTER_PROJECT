from typing import Optional, Type, TypeVar, Iterator, List, Tuple, Dict
from app import app
from app.db import get_db
from app.filter_parser import FilterParser, Params
from app.exceptions import ValidationError, MissingError, QueryParameterError

def _legacy_to_params(**kwargs) -> Dict:
    res = {}
    if 'and_filter' in kwargs:
        res['and_filter'] = kwargs['and_filter']
    if 'or_filter' in kwargs:
        res['or_filter'] = kwargs['or_filter']
    if 'group_by' in kwargs:
        res['group_by'] = kwargs['group_by']
    if 'order_by' in kwargs:
        res['order_by'] = kwargs['order_by']
    if 'limit' in kwargs:
        res['limit'] = kwargs['limit']
    if 'offset' in kwargs:
        res['offset'] = kwargs['offset']
    return res

T = TypeVar('T')

class BaseServices:
    model: Type[T] = None
    table_name = ''
    columns_to_update: set = set()
    MODEL_ALIAS_MAP = {}

    @staticmethod
    def _row_to_model(row, model_cls: Type[T]) -> T:
        if row is None:
            raise MissingError("Invalid Row Data")
        return model_cls(**row)

    @classmethod
    def get_by_id(cls, id: int) -> object:
        db = get_db()
        row = db.execute(
            f'SELECT * FROM {cls.table_name} WHERE id = ?', (id,)).fetchone()
        if row is None:
            # Safer name retrieval
            name = cls.model.__name__ if cls.model else cls.table_name
            raise MissingError(f"{name} not found in the database")
        return cls._row_to_model(row, cls.model)

    @classmethod
    def list_row_by_page(cls,
                         page: int,
                         params: Optional[Params] = None,
                         **kwargs,
                         ) -> Iterator:
        if page < 1:
            raise ValidationError("Page number must be >= 1")

        # Use .get() for safety, defaulting to 20 if config is missing
        default_limit = app.config.get('ADMIN_PAGE_PAGINATION', 20)
        limit = params.limit if params and params.limit > 0 else default_limit

        if limit < 0:
            raise ValidationError("Invalid Page limit")

        offset = (page - 1) * limit
        params = Params() if params is None else params
        params = params.set_limit(limit).set_offset(offset)
        return cls.get_all(params=params)

    @classmethod
    def update_data(cls, model: Type[T]) -> T:
        db = get_db()
        field = [f"{key}=?" for key in vars(
            model).keys() if key in cls.columns_to_update]
        values = [v for k, v in vars(
            model).items() if k in cls.columns_to_update]

        db.execute(
            f'UPDATE {cls.table_name} SET {",".join(field)} WHERE id = ?', values + [model.id])
        db.commit()
        return model

    @classmethod
    def get_total(cls,
                  params: Optional[Params] = None,
                  **kwargs) -> int:

        query = f'SELECT COUNT(*) from {cls.table_name}'
        res ={}
        if params is not None:
            if params.group_by or params.order_by:
                raise QueryParameterError("You can't groupby or order by to get_total")

            mapper = {cls.model: cls.table_name}
            res = FilterParser.parse_params(params, model_map=mapper)
        else:
            res = _legacy_to_params(**kwargs)

        query, args = cls._apply_filter(
            base_query=query,
            base_arg=[],
            **res
        )
        db = get_db()
        res =db.execute(query, args).fetchone()
        if res:
            return res[0]
        else:
            return 0

    @classmethod
    def _run_query(cls, query: str, params: list, row_mapper):
        db = get_db()
        rows = db.execute(query, params)
        return [row_mapper(row) for row in rows]


    @classmethod
    def _apply_filter(cls,
                      base_query: str,
                      base_arg: Optional[List] = None,
                      limit: int = 0,
                      offset: int = 0,
                      and_filter: Optional[List[Tuple]] = None,
                      or_filter: Optional[List[Tuple]] = None,
                      order_by: Optional[List[Tuple[str, str]]] = None,
                      group_by: Optional[List[str]] = None
                      ):
        ALLOWED_OPERATORS = {'=', '>', '<', '>=', '<=', '!=', 'LIKE', 'IN', 'BETWEEN'}
        query = ''
        args = base_arg if base_arg is not None else []
        conditions = []
        query = base_query

        if and_filter:
            for column_name, value, opt in and_filter:
                if opt.upper() not in ALLOWED_OPERATORS:
                    raise ValidationError(f"Invalid operator: {opt}")
                if opt.upper() == 'BETWEEN':
                    conditions.append(f"{column_name} {opt} ? AND ?")
                    args.extend(value)
                else:
                    conditions.append(f"{column_name} {opt} ?")
                    args.append(value)

        if or_filter:
            or_conditions = []
            for column_name, value, opt in or_filter:
                if opt.upper() not in ALLOWED_OPERATORS:
                    raise ValidationError(f"Invalid operator: {opt}")
                if opt.upper() == 'BETWEEN':
                    or_conditions.append(f"{column_name} {opt} ? AND ?")
                    args.extend(value)
                else:
                    or_conditions.append(f"{column_name} {opt} ?")
                    args.append(value)
            if or_conditions:
                conditions.append("(" + " OR ".join(or_conditions) + ")")


        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # --- GROUP BY Clause ---
        if group_by:
            query += f" GROUP BY {', '.join(group_by)}"
        # --- ORDER BY Clause ---
        if order_by:
            clause = []
            for col, direction in order_by:
                if direction.upper() not in ['ASC', 'DESC']:
                    raise ValidationError(
                        f"Invalid sort direction: {direction}")
                clause.append(f"{col} {direction.upper()}")
            query += f" ORDER BY {','.join(clause)}"

        if limit > 0:
            query += ' LIMIT ?'
            args.append(limit)

        if offset > 0:
            query += ' OFFSET ?'
            args.append(offset)

        return query, args

    @classmethod
    def get_all(cls,
                params: Optional[Params] = None,
                **kwargs
                ) -> Iterator:

        res = {}
        if params:
            model_map = {cls.model: cls.table_name}
            res = FilterParser.parse_params(params, model_map=model_map)
        else:
            res = _legacy_to_params(**kwargs)
        query = f"SELECT * from {cls.table_name}"
        query, args = cls._apply_filter(query,
                                        **res)
        db = get_db()
        rows = db.execute(query, args)
        for row in rows:
            yield cls._row_to_model(row, cls.model)

    @classmethod
    def has_next_page(cls, page: int,
                      page_count=None,
                      params: Optional[Params] =  None,
                      **kwargs
                      ) -> bool:

        if page_count is None:
            page_count = app.config.get('ADMIN_PAGE_PAGINATION', 20)

        res = {}
        if params:
            total = cls.get_total(params)
        else:
            total = cls.get_total(**kwargs)

        current = page * page_count
        if current < total:
            return True
        return False
