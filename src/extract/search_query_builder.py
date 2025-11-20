class SearchQueryBuilder:
    def __init__(self, table_name: str):
        if (len(table_name) == 0):
            raise ValueError("Table name should not be empty")

        self.table_name = table_name
        self.columns = []
        self.filters = []
        self.order = None
        self.limit = None
    
    def add_column(self, column_name: str):
        self.columns.append(f"{column_name}")
        return self

    def add_column_with_alias(self, column_name: str, alias: str):
        self.columns.append(f"{column_name} AS {alias}")
        return self

    def column_equals_to(self, column_name: str, value):
        self.filters.append(f"{column_name} = {value}")
        return self

    def column_greater_than(self, column_name: str, value):
        self.filters.append(f"{column_name} > {value}")
        return self

    def column_greater_than_or_equal(self, column_name: str, value):
        self.filters.append(f"{column_name} >= {value}")
        return self

    def column_less_than(self, column_name: str, value):
        self.filters.append(f"{column_name} < {value}")
        return self

    def column_less_than_or_equal(self, column_name: str, value):
        self.filters.append(f"{column_name} <= {value}")
        return self

    def column_not_equal(self, column_name: str, value):
        self.filters.append(f"{column_name} <> {value}")
        return self
    
    def order_by(self, column_name: str, direction="ASC"):
        self.order = f"{column_name} {direction}"
        return self
    
    def add_limit(self, limit_value):
        self.limit = limit_value
        return self
    
    def get_query(self) -> str:
        table_name = self.table_name
        columns = self.columns if self.columns else ["*"] # Default value if there are no columns in the query
        filters = self.filters
        order = self.order
        limit = self.limit

        query_string = f"SELECT {', '.join(columns)} FROM `{table_name}`"

        if filters:
            query_string += f" WHERE {' AND '.join(filters)}"

        if order:
            query_string += f" ORDER BY {order}"

        if limit:
            query_string += f" LIMIT {limit}"

        return query_string