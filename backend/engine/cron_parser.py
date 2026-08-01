import re
from typing import List, Set, Optional
from datetime import datetime, timedelta


class CronParseError(Exception):
    pass


class CronExpression:
    _MONTH_MAP = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }

    _DOW_MAP = {
        'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6
    }

    def __init__(self, expression: str):
        self.expression = expression.strip()
        parts = self.expression.split()
        if len(parts) != 5:
            raise CronParseError(f"Cron expression must have exactly 5 fields, got {len(parts)}: {expression}")

        self.minute = self._parse_field(parts[0], 0, 59)
        self.hour = self._parse_field(parts[1], 0, 23)
        self.day_of_month = self._parse_field(parts[2], 1, 31, self._MONTH_MAP)
        self.month = self._parse_field(parts[3], 1, 12, self._MONTH_MAP)
        self.day_of_week = self._parse_field(parts[4], 0, 6, self._DOW_MAP)

    def _parse_field(self, field: str, min_val: int, max_val: int,
                     name_map: Optional[dict] = None) -> Set[int]:
        result: Set[int] = set()
        field_lower = field.lower()

        for part in field_lower.split(','):
            result.update(self._parse_part(part, min_val, max_val, name_map))

        return result

    def _parse_part(self, part: str, min_val: int, max_val: int,
                    name_map: Optional[dict] = None) -> Set[int]:
        result: Set[int] = set()

        if name_map:
            for name, val in name_map.items():
                part = part.replace(name, str(val))

        if part == '*':
            return set(range(min_val, max_val + 1))

        step_match = re.match(r'^(.+)/(\d+)$', part)
        if step_match:
            base = step_match.group(1)
            step = int(step_match.group(2))
            if step <= 0:
                raise CronParseError(f"Step must be positive: {part}")

            if base == '*':
                start, end = min_val, max_val
            elif '-' in base:
                range_match = re.match(r'^(\d+)-(\d+)$', base)
                if not range_match:
                    raise CronParseError(f"Invalid range in step: {part}")
                start = int(range_match.group(1))
                end = int(range_match.group(2))
            else:
                start = int(base)
                end = max_val

            self._validate_value(start, min_val, max_val, part)
            self._validate_value(end, min_val, max_val, part)

            for val in range(start, end + 1, step):
                result.add(val)
            return result

        if '-' in part:
            range_match = re.match(r'^(\d+)-(\d+)$', part)
            if not range_match:
                raise CronParseError(f"Invalid range: {part}")
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            self._validate_value(start, min_val, max_val, part)
            self._validate_value(end, min_val, max_val, part)
            if start > end:
                for val in range(start, max_val + 1):
                    result.add(val)
                for val in range(min_val, end + 1):
                    result.add(val)
            else:
                for val in range(start, end + 1):
                    result.add(val)
            return result

        try:
            val = int(part)
            self._validate_value(val, min_val, max_val, part)
            result.add(val)
            return result
        except ValueError:
            raise CronParseError(f"Invalid field value: {part}")

    def _validate_value(self, val: int, min_val: int, max_val: int, context: str):
        if val < min_val or val > max_val:
            raise CronParseError(f"Value {val} out of range [{min_val}, {max_val}] in: {context}")

    def matches(self, dt: datetime) -> bool:
        return (
            dt.minute in self.minute and
            dt.hour in self.hour and
            dt.day in self.day_of_month and
            dt.month in self.month and
            dt.weekday() in self.day_of_week
        )

    def next_run(self, after: Optional[datetime] = None) -> datetime:
        if after is None:
            after = datetime.now()

        current = after.replace(second=0, microsecond=0) + timedelta(minutes=1)

        for _ in range(525600):
            if self.matches(current):
                return current
            current += timedelta(minutes=1)

        raise CronParseError(f"No next run found within 1 year for: {self.expression}")

    def next_runs(self, count: int = 5, after: Optional[datetime] = None) -> List[datetime]:
        runs = []
        current = after
        for _ in range(count):
            current = self.next_run(current)
            runs.append(current)
        return runs
