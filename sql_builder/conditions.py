"""Condition parsing for SQL WHERE clauses."""

import re
import itertools
from typing import Any, Dict, List, Tuple, Union, Callable
from .mappings import valid_operators

class Condition:
    """Represents a single SQL condition (e.g., col = value)."""
    _ids = itertools.count(1)
    __slots__ = ('field', 'op', 'values', '_uid', 'aggregate')

    def __init__(self, field: str, op: str, values: List[Any], aggregate: Optional[str] = None):
        """Initialize condition."""
        if not re.match(r'^[\w]+$', field):
            raise ValueError(f'Invalid field name: {field}')
        if op.upper() not in valid_operators:
            raise ValueError(f'Invalid operator: {op}')
        self.field = field
        self.op = op.upper()
        self.values = values if isinstance(values, list) else [values]
        self.aggregate = aggregate
        self._uid = next(self._ids)

    def to_sql(self, dialect: str, ph: str, quote_char: str, dt_wrapper: Callable) -> Tuple[str, Dict[str, Any]]:
        """Convert condition to SQL fragment and parameters."""
        col_str = f'{quote_char}{self.field}{quote_char}'
        if self.aggregate:
            col_str = f'{self.aggregate.upper()}({col_str})'
        params = {}
        pname_base = f'{self.field}_{self._uid}'

        if self.op in ('IS NULL', 'IS NOT NULL'):
            return f'{col_str} {self.op}', params

        if self.op == 'BETWEEN':
            params[f'{pname_base}_min'] = self.values[0]
            params[f'{pname_base}_max'] = self.values[1]
            return f'{col_str} BETWEEN {ph}{pname_base}_min AND {ph}{pname_base}_max', params

        if self.op in ('IN', 'NOT IN'):
            keys = [f'{ph}{pname_base}_{i}' for i in range(len(self.values))]
            params.update({f'{pname_base}_{i}': v for i, v in enumerate(self.values)})
            return f'{col_str} {self.op} ({",".join(keys)})', params

        if self.op in ('LIKE', 'ILIKE'):
            params[pname_base] = self.values[0]
            return f'{col_str} {self.op} {ph}{pname_base}', params

        if self.op in ('=', '!=', '<>', '<', '>', '<=', '>='):
            params[pname_base] = self.values[0]
            return f'{col_str} {self.op} {dt_wrapper(f"{ph}{pname_base}", self.op, self.values[0])}', params

        raise ValueError(f'Unsupported operator: {self.op}')

    @classmethod
    def from_input(cls, item: Any) -> 'Condition':
        """Create condition from various input types."""
        if isinstance(item, dict):
            aggregate = item.get('aggregate')
            return cls(item['field'], item['operator'], item.get('value', []), aggregate=aggregate)
        elif isinstance(item, str):
            return cls.from_string(item)
        elif isinstance(item, tuple):
            field, op, values = item
            if isinstance(values, str):
                values = [v.strip() for v in values.split(',')]
            return cls(field, op, values)
        elif isinstance(item, cls):
            return item
        raise TypeError(f'Unsupported condition type: {type(item)}')

    @classmethod
    def from_string(cls, text: str) -> 'Condition':
        """Parse condition from string (e.g., 'age > 30')."""
        tokens = _tokenize(text)
        if not tokens:
            raise ValueError('Empty condition')
        kind, value = tokens[0]
        if kind != 'IDENT':
            raise ValueError('Condition must start with field name')
        field = value
        if len(tokens) < 2:
            raise ValueError('Incomplete condition')
        op_kind, op_value = tokens[1]
        rest = tokens[2:]

        if op_kind == 'IS_NULL':
            return cls(field, 'IS NULL', [])
        if op_kind == 'IS_NOT_NULL':
            return cls(field, 'IS NOT NULL', [])
        if op_kind == 'COMP_OP':
            if not rest:
                raise ValueError('Missing value for comparison')
            val = _coerce(rest[0][1])
            return cls(field, op_value, [val])
        if op_kind in ('LIKE', 'ILIKE'):
            if not rest or rest[0][0] != 'STRING':
                raise ValueError('LIKE/ILIKE requires string value')
            return cls(field, op_value.upper(), [rest[0][1]])
        if op_kind == 'BETWEEN':
            if len(rest) != 3 or rest[1][1].upper() != 'AND':
                raise ValueError('BETWEEN requires <low> AND <high>')
            low = _coerce(rest[0][1])
            high = _coerce(rest[2][1])
            return cls(field, 'BETWEEN', [low, high])
        if op_kind in ('IN', 'NOT_IN'):
            if not rest or rest[0][0] != 'LP' or rest[-1][0] != 'RP':
                raise ValueError(f'{op_kind} requires (values)')
            vals = []
            i = 1
            while i < len(rest) - 1:
                tk, tv = rest[i]
                if tk in ('STRING', 'NUMBER'):
                    vals.append(_coerce(tv))
                i += 1
            return cls(field, op_value.upper(), vals)
        if op_kind == 'NOT' and rest and rest[0][0] == 'IN':
            rest = rest[1:]
            if not rest or rest[0][0] != 'LP' or rest[-1][0] != 'RP':
                raise ValueError('NOT IN requires (values)')
            vals = []
            i = 1
            while i < len(rest) - 1:
                tk, tv = rest[i]
                if tk in ('STRING', 'NUMBER'):
                    vals.append(_coerce(tv))
                i += 1
            return cls(field, 'NOT IN', vals)
        raise ValueError(f'Cannot parse condition: {text}')

def _coerce(val: str) -> Union[int, float, bool, str]:
    """Coerce string to appropriate type."""
    val = val.strip()
    if val.lower() in ('true', 'yes', 'y', '1'):
        return True
    if val.lower() in ('false', 'no', 'n', '0'):
        return False
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val

def _tokenize(text: str) -> List[Tuple[str, str]]:
    """Tokenize SQL condition string."""
    token_spec = [
        (r'\s+', None),
        (r'\bIS NOT NULL\b', 'IS_NOT_NULL'),
        (r'\bIS NULL\b', 'IS_NULL'),
        (r'\bNOT IN\b', 'NOT_IN'),
        (r'\bNOT\b', 'NOT'),
        (r'\bBETWEEN\b', 'BETWEEN'),
        (r'\bIN\b', 'IN'),
        (r'\bILIKE\b', 'ILIKE'),
        (r'\bLIKE\b', 'LIKE'),
        (r'!=|<>|<=|>=|>|<|=', 'COMP_OP'),
        (r'\(', 'LP'),
        (r'\)', 'RP'),
        (r"'(?:[^']|'')*'", 'STRING'),
        (r'\d+(?:\.\d+)?', 'NUMBER'),
        (r'\bAND\b', 'AND'),
        (r'\bOR\b', 'OR'),
        (r'[A-Za-z_]\w*', 'IDENT'),
        (r',', 'COMMA'),
    ]
    master_re = re.compile('|'.join(f'(?P<{name or "WHITESPACE"}>{pat})' for pat, name in token_spec), re.IGNORECASE)
    pos = 0
    out = []
    while pos < len(text):
        m = master_re.match(text, pos)
        if not m:
            raise SyntaxError(f'Unexpected char at {pos}: {text[pos:pos+10]}')
        pos = m.end()
        kind = m.lastgroup
        if kind is None or kind == 'WHITESPACE':
            continue
        value = m.group()
        if kind == 'STRING':
            value = value[1:-1].replace("''", "'")
        out.append((kind, value))
    return out