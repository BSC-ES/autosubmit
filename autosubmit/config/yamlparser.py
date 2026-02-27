# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.


from pathlib import Path

from ruamel.yaml import YAML

# ---------------------------------------------------------------------------
# Optional yaml-provenance integration
# ---------------------------------------------------------------------------
# When the ``yaml-provenance`` library is installed every value loaded from a
# YAML file becomes a ``WithProvenance`` subclass of its native type (str, int,
# …).  This means any downstream code can inspect *which* file, line and column
# a value originated from without any changes to the rest of Autosubmit.
#
# Install (from the feature branch until merged to main):
#   pip install "yaml-provenance @ git+https://github.com/esm-tools/yaml-provenance.git@feat/yaml_dumper"
#
# If the library is not installed, loading falls back silently to the standard
# ruamel.yaml behaviour so nothing breaks.
# ---------------------------------------------------------------------------
try:
    from yaml_provenance import load_yaml, configure, ProvenanceConfig
    from yaml_provenance._wrapper import (
        _wrapper_registry,
        BoolWithProvenance,
        NoneWithProvenance,
        ProvenanceClassForTheUnsubclassable,
    )
    from yaml_provenance._list import ListWithProvenance
    from yaml_provenance._dict import DictWithProvenance

    # Enable full provenance history so merges across multiple YAML files
    # preserve a complete chain of origin information.
    configure(ProvenanceConfig(track_history=True))

    _HAS_YAML_PROVENANCE = True
except ImportError:
    _HAS_YAML_PROVENANCE = False


# ---------------------------------------------------------------------------
# Pickle compatibility for WithProvenance types
# ---------------------------------------------------------------------------
# yaml-provenance creates wrapper classes dynamically at runtime via
# ``type(class_name, (base_type,), {...})``.  The classes are stored only in
# ``_wrapper_registry`` — they are *never* added as attributes of the
# ``yaml_provenance._wrapper`` module.  Pickle, however, serialises a class
# by recording its module + qualified name and then performing an attribute
# lookup on that module at restore time:
#
#     yaml_provenance._wrapper.StrWithProvenance   →   AttributeError
#
# This causes ``_pickle.PicklingError`` when Autosubmit tries to persist the
# job list (``job_list_persistence.py``, line 130).
#
# Fix: inject a ``__reduce__`` method into every WithProvenance class so that
# pickle serialises each instance as its plain builtin base type (str, int,
# float, bool …).  Provenance metadata is intentionally dropped during pickle
# — it is only used in-process for diagnostics, not for job execution.
#
# The function must be called *after every* ``load_yaml()`` invocation because
# the registry is populated lazily: new entry types (e.g.
# ``ScalarIntWithProvenance``, ``ScalarFloatWithProvenance``) are added the
# first time a value of that type is encountered.  Re-patching is idempotent.
# ---------------------------------------------------------------------------

# Builtin types we want to reduce to (walking up MRO past ruamel scalars).
_BUILTIN_TYPES = (str, int, float, bytes, bytearray)


def _get_builtin_base(cls):
    """Return the first plain builtin ancestor of *cls* in its MRO."""
    for base in cls.__mro__:
        if base in _BUILTIN_TYPES:
            return base
    # Fallback — should never happen for types in _wrapper_registry.
    return cls.__bases__[0]


def _register_provenance_pickle_reducers():
    """Patch __reduce__ on every WithProvenance class so pickle can handle them.

    Safe to call multiple times; only patches classes that have not already
    been patched (detected by the presence of the ``_pickle_patched`` sentinel).
    """
    if not _HAS_YAML_PROVENANCE:
        return

    def _make_reduce(builtin_type):
        """Return a __reduce__ that serialises self as a plain builtin value."""
        def __reduce__(self):
            return (builtin_type, (builtin_type(self),))
        return __reduce__

    # 1. Dynamic registry types: StrWithProvenance, IntWithProvenance,
    #    ScalarIntWithProvenance, ScalarFloatWithProvenance, and any others
    #    that get added as more YAML files are loaded.
    for cls in _wrapper_registry.values():
        if not getattr(cls, "_pickle_patched", False):
            builtin = _get_builtin_base(cls)
            cls.__reduce__ = _make_reduce(builtin)
            cls._pickle_patched = True

    # 2. BoolWithProvenance — stores the value in self.value, not as the
    #    bool itself (bool cannot be subclassed, hence the wrapper class).
    if not getattr(BoolWithProvenance, "_pickle_patched", False):
        BoolWithProvenance.__reduce__ = lambda self: (bool, (self.value,))
        BoolWithProvenance._pickle_patched = True

    # 3. NoneWithProvenance — rare in AS configs but patch for completeness.
    if not getattr(NoneWithProvenance, "_pickle_patched", False):
        NoneWithProvenance.__reduce__ = lambda self: (type(None), ())
        NoneWithProvenance._pickle_patched = True

    # 4. ListWithProvenance — items may themselves be WithProvenance instances;
    #    reducing to a plain list lets pickle recurse into the (now reducible)
    #    items automatically.
    if not getattr(ListWithProvenance, "_pickle_patched", False):
        ListWithProvenance.__reduce__ = lambda self: (list, (list(self),))
        ListWithProvenance._pickle_patched = True

    # 5. DictWithProvenance — same reasoning as ListWithProvenance.
    if not getattr(DictWithProvenance, "_pickle_patched", False):
        DictWithProvenance.__reduce__ = lambda self: (dict, (dict(self),))
        DictWithProvenance._pickle_patched = True


def _register_provenance_yaml_representers():
    """Register WithProvenance types on ruamel.yaml representer classes.

    ruamel.yaml's ``represent_data()`` dispatches by exact type first, then
    walks the MRO only in ``yaml_multi_representers`` — but all builtin type
    representers (str, int, bool, …) live in ``yaml_representers``.  This
    means ``StrWithProvenance`` is not found via MRO walk and falls through to
    the ``None`` catch-all representer which raises
    ``cannot represent an object``.

    Fix: register each WithProvenance class directly in the representer CLASS
    dict (not instance), so every new ``YAML()`` instance inherits the mapping.
    Called after every ``load_yaml()`` to pick up newly registered types.
    """
    if not _HAS_YAML_PROVENANCE:
        return

    try:
        from ruamel.yaml.representer import SafeRepresenter, RoundTripRepresenter
    except ImportError:
        return

    if not hasattr(_register_provenance_yaml_representers, "_patched"):
        _register_provenance_yaml_representers._patched = set()
    patched = _register_provenance_yaml_representers._patched

    for repr_class in (SafeRepresenter, RoundTripRepresenter):

        # 1. Dynamic registry types (StrWithProvenance, IntWithProvenance, etc.)
        for cls_name, cls in _wrapper_registry.items():
            key = (repr_class, cls_name)
            if key in patched:
                continue
            builtin = _get_builtin_base(cls)
            fn = repr_class.yaml_representers.get(builtin)
            if fn:
                repr_class.add_representer(cls, fn)
                patched.add(key)

        # 2. BoolWithProvenance — not in _wrapper_registry, handled separately.
        key = (repr_class, "BoolWithProvenance")
        if key not in patched:
            bool_fn = repr_class.yaml_representers.get(bool)
            if bool_fn:
                repr_class.add_representer(
                    BoolWithProvenance,
                    lambda dumper, data, _fn=bool_fn: _fn(dumper, data.value),
                )
                patched.add(key)

        # 3. NoneWithProvenance
        key = (repr_class, "NoneWithProvenance")
        if key not in patched:
            none_fn = repr_class.yaml_representers.get(type(None))
            if none_fn:
                repr_class.add_representer(
                    NoneWithProvenance,
                    lambda dumper, data, _fn=none_fn: _fn(dumper, None),
                )
                patched.add(key)

        # 4. ListWithProvenance — convert to plain list so the representer
        #    recurses into (now representable) items.
        key = (repr_class, "ListWithProvenance")
        if key not in patched:
            list_fn = repr_class.yaml_representers.get(list)
            if list_fn:
                repr_class.add_representer(
                    ListWithProvenance,
                    lambda dumper, data, _fn=list_fn: _fn(dumper, list(data)),
                )
                patched.add(key)

        # 5. DictWithProvenance
        key = (repr_class, "DictWithProvenance")
        if key not in patched:
            dict_fn = repr_class.yaml_representers.get(dict)
            if dict_fn:
                repr_class.add_representer(
                    DictWithProvenance,
                    lambda dumper, data, _fn=dict_fn: _fn(dumper, dict(data)),
                )
                patched.add(key)


class YAMLParserFactory:
    def __init__(self):
        pass

    def create_parser(self):
        return YAMLParser()


class YAMLParser(YAML):

    def __init__(self):
        self.data = []
        super(YAMLParser, self).__init__(typ="rt")
