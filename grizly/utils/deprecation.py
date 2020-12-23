from typing import Dict
from functools import wraps
import warnings


def deprecated_params(params_mapping: Dict[str, str], deprecated_in: str, removed_in: str):
    """Raise DeprecationWarning if some parameters are found"""

    def deco_wrap(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            for old_param, new_param in params_mapping.items():
                param_is_found = kwargs.get(old_param) is not None
                if param_is_found:
                    kwargs[new_param] = kwargs.get(old_param)
                    if f.__name__ == "__init__":
                        func_name = f.__class__.__name__
                    else:
                        func_name = f.__name__
                    message = (
                        f"Parameter {old_param} in {func_name} is deprecated"
                        f" as of {deprecated_in} and will be removed in"
                        f" {removed_in}. Please use {new_param} instead"
                    )
                    warnings.warn(message, DeprecationWarning, stacklevel=2)

            return f(*args, **kwargs)

        return wrapped

    return deco_wrap
