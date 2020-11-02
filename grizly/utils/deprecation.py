from typing import Dict, Callable, Union
from functools import wraps
import warnings


def deprecated_params(
    params_mapping: Dict[str, Union[str, None]], deprecated_in: str, removed_in: str
):
    """Raise DeprecationWarning if some parameters are found"""

    def deco_wrap(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            for old_param, new_param in params_mapping.items():
                param_is_found = kwargs.get(old_param) is not None
                if param_is_found:
                    if new_param is not None:
                        kwargs[new_param] = kwargs.get(old_param)
                    func_name = __get_function_name(f)
                    message = __get_message(
                        old_param, new_param, func_name, deprecated_in, removed_in
                    )
                    warnings.warn(message, DeprecationWarning, stacklevel=2)

            return f(*args, **kwargs)

        return wrapped

    return deco_wrap


def __get_function_name(f: Callable) -> str:
    if f.__name__ == "__init__":
        func_name = f.__class__.__name__
    else:
        func_name = f.__name__
    return func_name


def __get_message(
    old_param: str, new_param: Union[str, None], func_name: str, deprecated_in: str, removed_in: str
) -> str:
    message = (
        f"Parameter {old_param} in {func_name} is deprecated"
        f" as of {deprecated_in} and will be removed in {removed_in}."
    )
    if new_param is not None:
        message += f" Please use {new_param} instead."
    return message
