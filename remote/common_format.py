from decimal import Decimal


def format_value(value):
    if isinstance(value, Decimal):
        if value.is_zero():
            return "0"
        s = format(value, "f")
        s = s.rstrip("0").rstrip(".")
        if not s or s == "-":
            return "0"
        return s.replace(".", ",")
    if isinstance(value, float):
        if value == 0.0:
            return "0"
        s = f"{value:.15f}".rstrip("0").rstrip(".")
        if not s or s == "-":
            return "0"
        return s.replace(".", ",")
    return value
