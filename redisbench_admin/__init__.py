#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

# This attribute is the only one place that the version number is written down,
# so there is only one place to change it when the version number changes.

try:
    from importlib.metadata import version
except ImportError:
    try:  # For Python<3.8
        from importlib_metadata import version  # type: ignore
    except ImportError:
        version = None

try:
    __version__ = version("redisbench-admin")
except Exception:
    __version__ = "99.99.99"  # like redis modules
