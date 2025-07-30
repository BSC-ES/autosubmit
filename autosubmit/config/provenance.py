"""
Provenance's dark magic. The basic idea is that one use the following to understand
from which yaml file (line and column) a variable in ``config`` is coming from:

.. code-block:: python

    config["fesom"]["version"].provenance

And that will return a list of the provenance history of that variable, for example:

.. code-block:: python

    [{'category': 'components',
      'col': 10,
      'line': 6,
      'yaml_file': '/Users/mandresm/Codes/esm_tools/configs/components/fesom/fesom-2.0.yaml'},
     {'category': 'setups',
      'col': 18,
      'extended_by': 'dict.__setitem__',
      'line': 321,
      'yaml_file': '/Users/mandresm/Codes/esm_tools/configs/setups/awicm3/awicm3.yaml'}]

The last element in the provenance list represents the provenance of the current value
(last provenance).

This module contains:
* The provenance class, to store the provenance of values with extended functionality
* A wrapper factory to create classes and objects dynamically that subclass the value
    types and append provenances to them (``WithProvenance`` classes)
* Class attributes common to all ``WithProvenance`` classes
* Classes for mappings with provenance (dictionaries and lists) to recursively put and
    get provenance from nested values, and extend the standard mapping methods
    (``__setitem__``, ``update``...)
* A decorator to keep provenance in ``esm_parser``'s recursive functions
* A method to clean provenance recursively and get back the data without provenance
"""

import copy

# TODO: what to do about these?

import esm_parser
from esm_calendar import Date


# =================
# PROVENANCE CLASS
# =================
class Provenance(list):
    """
    A subclass of list in which each element represents the provenance of the value
    at a point in the key-value history. The whole point of this class is to have a
    list subclass that allows us to include information about which function is
    changing the list within each provenance element.

    To assign the provenance to a value, instanciate it as an attribute of that value
    (i.e. ``self.provenance = Provenance(my_provenance)``). To be used from the
    ``WithProvenance`` classes created by ``wrapper_with_provenance_factory``.

    The following class methods provide the extended functionality to lists:
    * ``self.append_last_step_modified_by``: to duplicate the last element of the list
        and add to it information about the function that is modifying the value
    * ``self.extend_and_modified_by``: to extend a list while including in the
        provenance the function which is responsible for extending it
    """

    def __init__(self, provenance_data):
        """
        Initialize the provenance object with its subclass (``list``) giving
        ``provenance_data`` as input. If ``provenance_data`` is not a ``list``
        initialize the object as a list which only element is ``provenance_data``.

        Parameters
        ----------
        provenance_data : list
            List of provenance elements that describes the history of a key-value, or
            a single provenance element.
        """

        if isinstance(provenance_data, list):
            super().__init__(provenance_data)
        else:
            super().__init__([provenance_data])

    def append_last_step_modified_by(self, func):
        """
        Copies the last element in the provenance history and adds the entry ``modify_by``
        with value ``func`` to the copy.

        Parameters
        ----------
        func : str
            Function that is modifying the variable
        """
        new_provenance_step = copy.deepcopy(self[-1])
        new_provenance_step = self.add_modified_by(new_provenance_step, func)

        self.append(new_provenance_step)

    def extend_and_modified_by(self, additional_provenance, func):
        """
        Extends the current provenance history with an ``additional_provenance``. This
        happens when for example a variable comes originally from a file, but then the
        value is overwritten by another value that comes from a file higher in the
        hierarchy. This method keeps both histories, with the history of the second been
        on top of the first.

        Parameters
        ----------
        additional_provenance : esm_parser.Provenance
            Additional provenance history to be used for extending ``self``
        func : str
            Function triggering this method
        """
        new_additional_provenance = additional_provenance
        # If the new provenance is not identical to the current one extend the
        # provenance
        if new_additional_provenance is not self:
            for elem in new_additional_provenance:
                new_additional_provenance.add_modified_by(
                    elem, func, modified_by="extended_by"
                )
            self.extend(new_additional_provenance)
        # If the new provenance is identical just mark the variable as modified_by
        # func
        else:
            self.append_last_step_modified_by(func)

    def add_modified_by(self, provenance_step, func, modified_by="modified_by"):
        """
        Adds a variable of name defined by ``modified_by`` to the given provenance step
        with value ``func``. This variable is used to label provenance steps of the
        provenance history with functions that modified it.

        Parameters
        ----------
        provenance_step : dict
            Provenance entry of the current step
        func : str
            Function triggering this method
        modified_by : str
            Name of the key for the labelling the type of modification

        Returns
        -------
        provenance_step : dict
            Provenance entry of the current step with the ``modified_by`` item
        """
        if provenance_step is not None:
            provenance_step[modified_by] = str(func)

        return provenance_step


# ========================================================
# PROVENANCE WRAPPER FACTORY CLASS METHODS AND PROPERTIES
# ========================================================
@classmethod
def wrapper_with_provenance_new(cls, *args, **kwargs):
    """
    To be used as the ``__new__`` method for WithProvenance classes. This is key for
    ``copy.deepcopy``, without this ``copy.deepcopy`` breaks.
    """
    return super(cls, cls).__new__(cls, args[1])


def wrapper_with_provenance_init(self, value, provenance=None):
    """
    To be used as the ``__init__`` method for WithProvenance classes. Adds the
    ``provenance`` value as an instance of ``Provenance`` to the ``self._provenance``
    attribute, and stores the original ``value`` to the ``self.value`` attribute.

    Parameters
    ----------
    value : any
        Value of the object
    provenance : any
        The provenance information
    """
    self._provenance = Provenance(provenance)
    self.value = value


@property
def prop_provenance(self):
    """
    To be used as the ``provenance`` property in WithProvenance classes.

    Returns
    -------
    self._provenance : esm_parser.provenance.Provenance
        The provenance history stored in ``self._provenance``
    """
    return self._provenance


@prop_provenance.setter
def prop_provenance(self, new_provenance):
    """
    Setter for the ``provenance`` property of WithProvenance classes. Makes sure that
    any value assigned to this property is a ``Provenance`` object and if it is not
    returns an error.

    Parameters
    ----------
    new_provenance : esm_parser.provenance.Provenance
        New provenance history to be set

    Raises
    ------
    ValueError :
        If the given ``new_provenance`` is not a ``Provenance`` object
    """
    # Check if new_provenance is an instance of Provenance
    if not isinstance(new_provenance, Provenance):
        raise ValueError(
            "Provenance must be an instance of the provenance.Provenance class!"
        )

    self._provenance = new_provenance


# =======================================================
# CLASSES FOR THE UNSUBCLASSABLE CLASSES (BOOL AND NONE)
# =======================================================
class ProvenanceClassForTheUnsubclassable:
    """
    A class to reproduce the methods of the unclassable ``bool`` and ``NoneType``
    classes, needed to add the ``provenance`` attribute to those versions of the types.

    This class stores 2 attributes, one is the ``value`` of the target object (a
    ``bool`` or a ``NoneType``) and the other is the ``provenance``.
    """

    def __repr__(self):
        return f"{self.value}"

    def __bool__(self):
        return bool(self.value)

    def __eq__(self, other):
        if isinstance(other, BoolWithProvenance):
            return self.value == other.value

        return self.value == other

    def __hash__(self):
        return hash(self.value)


# Add the class attributes that are common to all WithProvenance classes
ProvenanceClassForTheUnsubclassable.__init__ = wrapper_with_provenance_init
ProvenanceClassForTheUnsubclassable.provenance = prop_provenance


class BoolWithProvenance(ProvenanceClassForTheUnsubclassable):
    """
    Class for emulating ``Bool`` behaviour, but with Provenance.

    Objects of this class reproduce the following ``Bool`` behaviours:
    * ``isinstance(<obj>, bool)`` returns ``True``
    * ``<True_obj> == True`` returns ``True``
    * ``<True_obj> is True`` returns ``False``. This is not reproducing the behavior!
    """

    @property
    def __class__(self):
        """
        This is here for having ``isinstance(<my_bool_with_provenance>, bool)`` return ``True``
        """
        return bool


class NoneWithProvenance(ProvenanceClassForTheUnsubclassable):
    """
    Class for emulating ``None`` behaviour, but with Provenance.

    Objects of this class reproduce the following ``None`` behaviours:
    * ``isinstance(<obj>, None)`` returns ``True``
    * ``<obj> == None`` returns ``True``
    * ``<obj> is None`` returns ``False``. This is not reproducing the behavior!
    """

    @property
    def __class__(self):
        """
        This is here for having ``isinstance(<my_bool_with_provenance>, None)`` return
        ``True``
        """
        return type(None)


# ================================
# WRAPPER WITH PROVENANCE FACTORY
# ================================
def wrapper_with_provenance_factory(value, provenance=None):
    """
    A function to subclass and instantiate all types of subclassable objects in the
    ESM-Tools ``config`` and add the ``provenance`` attribute to them. It also creates
    the ``{type(value)}WithProvenance`` classes globally on the fly depending on the
    ``value``'s type, if it doesn't exist yet. For classes that are not subclassable
    (``Date``, ``Bool`` and ``NoneType``) instantiates an object that mimics their
    behaviour but also contains the ``provenance`` attribute.

    Objects of type ``esm_calendar.esm_calendar.Date`` are not subclassed - and the
    ``provenance`` attribute is simply added to them, because they fail to be subclassed
    with in the ``DateWithProvenance`` with the following error::

        __new__ method giving error object.__new__() takes exactly one argument
        (the type to instantiate)

    Parameters
    ----------
    value : any
        Value of the object to be subclassed and reinstantiated
    provenance : any
        The provenance information

    Returns
    -------
    {type(value)}WithProvenance, esm_calendar.esm_calendar.Date, BoolWithProvenance,
    NoneWithProvenance
        The new instance with the ``provenance`` attribute
    """

    if type(value) == bool:
        return BoolWithProvenance(value, provenance)

    elif value is None:
        return NoneWithProvenance(value, provenance)

    elif type(value) == Date:
        value.provenance = prop_provenance
        value.provenance = Provenance(provenance)

        return value

    elif isinstance(value, PROVENANCE_MAPPINGS):
        return value

    else:
        subtype = type(value)
        class_name = f"{subtype}".split("'")[1]
        class_name = f"{class_name[0].upper()}{class_name[1:]}WithProvenance"

        if class_name in globals():
            pass
        else:
            globals()[class_name] = type(
                class_name,
                (subtype,),
                {
                    "_class_name": class_name,
                    "__new__": wrapper_with_provenance_new,
                    "__init__": wrapper_with_provenance_init,
                    "provenance": prop_provenance,
                },
            )

        # Instantiate the subclass with the given value and provenance
        return globals()[class_name](value, provenance)


# =========================
# MAPPINGS WITH PROVENANCE
# =========================
class DictWithProvenance(dict):
    """
    A dictionary subclass that contains methods for:
    * recursively transforming leaf values into provenance (``put_provenance`` and
        ``set_provenance``)
    * recursively retrieving provenance from nested values
    * extending the ``dict.__init__`` method to recursively assign provenance to all
        nested values
    * extending the ``dict.__setitem__`` method to keep a record of previous history
        when adding new values to a given key
    * extending the ``dict.update`` method to keep a record of the previous history
        when updating the dictionary

    Use
    ---
    Instance a new ``DictWithProvenance`` object::

        dict_with_provenance = DictWithProvenance(<a_dictionary>, <my_provenance>)

    Redefine the provenance of an existing ``DictWithProvenance`` with the same
    provenance for all its nested values::

        dict_with_provenance.set_provenance(<new_provenance>)

    Set the provenace of a specific leaf within a nested dictionary::

        dict_with_provenance["key1"]["key1"].provenance = <new_provenance>

    Get the ``provenance`` representation of the whole dictionary::

        provenance_dict = dict_with_provenance.get_provenance()
    """

    def __init__(self, dictionary, provenance):
        """
        Instanciates the ``dictionary`` as an object of ``DictWithProvenance`` and
        defines its nested values as objects of WithProvenance classes, assigning them
        recursively the corresponding ``provenance`` attribute with ``set_provenance``.

        Parameters
        ----------
        dictionary : dict
            The ``dict`` that needs to be converted to a ``DictWithProvenance`` object
        provenance : dict
            The provenance that will be recursively assigned to all leaves of the
            dictionary tree
        """

        super().__init__(dictionary)

        self.custom_setitem = False
        self.put_provenance(provenance)
        self.custom_setitem = True

    def put_provenance(self, provenance):
        """
        Recursively transforms every value in ``DictWithProvenance`` into its
        corresponding WithProvenance object and appends its corresponding
        ``provenance``. Each value has its corresponding provenance defined in the
        ``provenance`` dictionary, and this method just groups them together 1-to-1.

        Parameters
        ----------
        provenance : dict
            The provenance that will be recursively assigned to all leaves of the
            dictionary tree. The provenance needs to be a ``dict`` with the same keys
            as ``self`` (same structure) so that it can successfully transfer each
            provenance value to its corresponding value on ``self`` (1-to-1
            conrrespondance).
        """

        for key, val in self.items():
            if isinstance(val, dict):
                self[key] = DictWithProvenance(val, provenance.get(key, {}))
            elif isinstance(val, list):
                self[key] = ListWithProvenance(val, provenance.get(key, []))
            elif hasattr(val, "provenance"):
                self[key].provenance.extend(provenance.get(key, {}))
            else:
                self[key] = wrapper_with_provenance_factory(
                    val, provenance.get(key, None)
                )

    def set_provenance(self, provenance):
        """
        Recursively transforms every value in ``DictWithProvenance`` into its
        corresponding WithProvenance object and appends the same ``provenance`` to it.
        Note that this method differs from ``put_provenance`` in that the same
        ``provenance`` value is applied to the different values of ``self``.

        Parameters
        ----------
        provenance : any
            New `provenance value` to be set
        """
        if not isinstance(provenance, list):
            provenance = [provenance]

        for key, val in self.items():
            if isinstance(val, dict):
                self[key] = DictWithProvenance(val, {})
                self[key].set_provenance(provenance)
            elif isinstance(val, list):
                self[key] = ListWithProvenance(val, [])
                self[key].set_provenance(provenance)
            elif hasattr(val, "provenance"):
                self[key].provenance.extend(provenance)
            else:
                self[key] = wrapper_with_provenance_factory(val, provenance)

    def get_provenance(self, index=-1):
        """
        Returns a ``dictionary`` containing the all the nested provenance information
        of the current ``DictWithProvenance`` with a structure and `keys` equivalent to
        the ``self`` dictionary, but with `values` of the `key` leaves those of the
        provenance.

        Parameters
        ----------
        index : int
            Defines the element of the provenance history to be returned. The default
            is ``-1``, meaning the last provenance (the one of the current value).

        Returns
        -------
        provenance_dict : dict
            A dictionary with a structure and `keys` equivalent to the ``self``
            dictionary, but with `values` of the `key` leaves those of the provenance
        """

        provenance_dict = {}

        for key, val in self.items():
            if isinstance(val, PROVENANCE_MAPPINGS):
                provenance_dict[key] = val.get_provenance(index=index)
            elif hasattr(val, "provenance"):
                provenance_dict[key] = val.provenance[index]
            else:
                # The DictWithProvenance object might have dictionaries inside that
                # are not instances of that class (i.e. a dictionary added in the
                # backend). The provenance in this method is then defined as None
                provenance_dict[key] = None

        return provenance_dict

    def __setitem__(self, key, val):
        """
        Any time an item in a DictWithProvenance is set, extend the old provenance of
        the old value with the provenance of the new ``val`` and make that be the new
        extended provenance history of the value.

        Parameters
        ----------
        key : str
            Key of the item
        val : any
            Value of the item
        """
        val_new = val
        if (
            key in self
            and not isinstance(self[key], (dict, list))
            and hasattr(self[key], "provenance")
            and hasattr(self, "custom_setitem")
            and self.custom_setitem
        ):
            new_provenance = self[key].provenance
            if hasattr(val, "provenance"):
                new_provenance.extend_and_modified_by(
                    val.provenance, "dict.__setitem__"
                )
                val_new = copy.deepcopy(val)
                val_new.provenance = new_provenance

        super().__setitem__(key, val_new)

    def update(self, dictionary, *args, **kwargs):
        """
        Preserves the provenance history when using the ``update`` method

        Parameters
        ----------
        dictionary : dict, esm_parser.provenance.DictWithProvenance
            Dictionary that will update ``self``
        """

        new_provs = {}

        for key, val in dictionary.items():
            if (
                key in self
                and not isinstance(self[key], (dict, list))
                and hasattr(self[key], "provenance")
                and hasattr(self, "custom_setitem")
                and self.custom_setitem
            ):
                new_provenance = self[key].provenance
                if hasattr(val, "provenance"):
                    new_provenance.extend_and_modified_by(val.provenance, "dict.update")
                    new_provs[key] = new_provenance

        super().update(dictionary, *args, **kwargs)

        for key, val in new_provs.items():
            self[key].provenance = val

DictWithProvenance.yaml_dump = esm_parser.yaml_dump


class ListWithProvenance(list):
    """
    A list subclass that contains methods for:
    * recursively transforming leaf values into provenance (``put_provenance`` and
        ``set_provenance``)
    * recursively retrieving provenance from nested values
    * extending the ``list.__init__`` method to recursively assign provenance to all
        nested values
    * extending the ``list.__setitem__`` method to keep a record of previous history
        when adding new values to a given key

    Use
    ---
    Instance a new ``ListWithProvenance`` object::

        list_with_provenance = ListWithProvenance(<a_list>, <my_provenance>)

    Redefine the provenance of an existing ``ListWithProvenance`` with the same
    provenance for all its nested values::

        list_with_provenance.set_provenance(<new_provenance>)

    Set the provenace of the element 0 of a list::

        list_with_provenance[0].provenance = <new_provenance>

    Get the ``provenance`` representation of the whole list::

        provenance_list = list_with_provenance.get_provenance()
    """

    def __init__(self, mylist, provenance):
        """
        Instanciates the ``list`` as an object of ``ListWithProvenance`` and defines
        its nested values as objects of WithProvenance classes, assigning them
        recursively the corresponding ``provenance`` attribute with ``set_provenance``.

        Parameters
        ----------
        mylist : list
            The ``list`` that needs to be converted to a ``ListWithProvenance`` object
        provenance : list
            The provenance that will be recursively assigned to all leaves of the
            dictionary tree
        """

        super().__init__(mylist)

        self.custom_setitem = False
        self.put_provenance(provenance)
        self.custom_setitem = True

    def put_provenance(self, provenance):
        """
        Recursively transforms every value in ``ListWithProvenance`` into its
        corresponding WithProvenance object and appends its corresponding
        ``provenance``. Each value has its corresponding provenance defined in the
        ``provenance`` list, and this method just groups them together 1-to-1.

        Parameters
        ----------
        provenance : list
            The provenance that will be recursively assigned to all elements of the
            list. The provenance needs to be a ``list`` with the same number of elements
            as ``self`` (same structure) so that it can successfully transfer each
            provenance value to its corresponding value on ``self`` (1-to-1
            conrrespondance).
        """

        if not provenance:
            provenance = [{}] * len(self)

        for c, elem in enumerate(self):
            if isinstance(elem, dict):
                self[c] = DictWithProvenance(elem, provenance[c])
            elif isinstance(elem, list):
                self[c] = ListWithProvenance(elem, provenance[c])
            elif hasattr(elem, "provenance"):
                self[c].provenance.extend(provenance[c])
            else:
                self[c] = wrapper_with_provenance_factory(elem, provenance[c])

    def set_provenance(self, provenance):
        """
        Recursively transforms every value in ``ListWithProvenance`` into its
        corresponding WithProvenance object and appends the same ``provenance`` to it.
        Note that this method differs from ``put_provenance`` in that the same
        ``provenance`` value is applied to the different values of ``self``.

        Parameters
        ----------
        provenance : any
            New `provenance value` to be set
        """
        if not isinstance(provenance, list):
            provenance = [provenance]

        for c, elem in enumerate(self):
            if isinstance(elem, dict):
                self[c] = DictWithProvenance(elem, {})
                self[c].set_provenance(provenance)
            elif isinstance(elem, list):
                self[c] = ListWithProvenance(elem, [])
                self[c].set_provenance(provenance)
            elif hasattr(elem, "provenance"):
                self[c].provenance.extend(provenance)
            else:
                self[c] = wrapper_with_provenance_factory(elem, provenance)

    def get_provenance(self, index=-1):
        """
        Returns a ``list`` containing the all the nested provenance information
        of the current ``ListWithProvenance`` with a structure equivalent to the
        ``self`` list, but with list elements been provenance values.

        Parameters
        ----------
        index : int
            Defines the element of the provenance history to be returned. The default
            is ``-1``, meaning the last provenance (the one of the current value).

        Returns
        -------
        provenance_list : list
            A list with a structure equivalent to that of the ``self`` list, but with
            the `values` of the provenance of each element
        """

        provenance_list = []

        for elem in self:
            if isinstance(elem, PROVENANCE_MAPPINGS):
                provenance_list.append(elem.get_provenance(index=index))
            elif hasattr(elem, "provenance"):
                provenance_list.append(elem.provenance[index])
            else:
                # The DictWithProvenance object might have dictionaries inside that
                # are not instances of that class (i.e. a dictionary added in the
                # backend). The provenance in this method is then defined as None
                provenance_list.append(None)

        return provenance_list

    def __setitem__(self, indx, val):
        """
        Any time an item in a ListWithProvenance is set, extend the old provenance of
        the old value with the provenance of the new ``val`` and make that be the new
        extended provenance history of the value.

        Parameters
        ----------
        indx : int
            Index of the element
        val : any
            Value of the item
        """
        val_new = val
        if (
            indx in self
            and not isinstance(self[indx], (dict, list))
            and hasattr(self[indx], "provenance")
            and hasattr(self, "custom_setitem")
            and self.custom_setitem
        ):
            new_provenance = self[indx].provenance
            if hasattr(val, "provenance"):
                new_provenance.extend_and_modified_by(
                    val.provenance, "dict.__setitem__"
                )
                val_new = copy.deepcopy(val)
                val_new.provenance = new_provenance

        super().__setitem__(indx, val_new)

ListWithProvenance.yaml_dump = esm_parser.yaml_dump


# Define the global variable PROVENANCE_MAPPINGS for operations such as ``isinstance``
PROVENANCE_MAPPINGS = (DictWithProvenance, ListWithProvenance)


# ==============================================
# DECORATORS FOR ESM_PARSER RECURSIVE FUNCTIONS
#   e.g. find_variable, purify_booleans...
# ==============================================
def keep_provenance_in_recursive_function(func):
    """
    Decorator for recursive functions in ``esm_parser`` to preserve
    provenance.

    Parameters
    ----------
    func : Callable
        The function to decorate
    """

    does_not_modify_prov = ["find_variable", "recursive_run_function"]
    modify_prov = not func.__name__ in does_not_modify_prov

    def inner(tree, rhs, *args, **kwargs):
        custom_setitem_was_turned_off_in_this_instance = False
        if hasattr(rhs, "custom_setitem") and rhs.custom_setitem:
            rhs.custom_setitem = False
            custom_setitem_was_turned_off_in_this_instance = True

        output = func(tree, rhs, *args, **kwargs)

        if hasattr(rhs, "provenance"):
            provenance = copy.deepcopy(rhs.provenance)
            # Value was modified
            if type(rhs) != type(output) or rhs != output:
                output = copy.deepcopy(output)
                # If the new value has an inherited provenance, keep it (i.e. variable
                # was called: rhs = ${fesom.namelist_dir}, output =
                # /actual/path/with/provenance/to/be/kept})
                if hasattr(output, "provenance"):
                    if modify_prov:
                        provenance.extend_and_modified_by(output.provenance, func)
                    output.provenance = provenance
                # If the rhs.provenance is not None and output has no provenance, keep
                # the old provenance
                elif provenance is not None:
                    if modify_prov:
                        provenance.append_last_step_modified_by(func)
                    output = wrapper_with_provenance_factory(output, provenance)

        if custom_setitem_was_turned_off_in_this_instance:
            rhs.custom_setitem = True

        return output

    return inner


# ========
# HELPERS
# ========
def clean_provenance(data):
    """
    Returns the values of provenance mappings in their original classes (without the
    provenance). Recurs through mappings. Make sure you copy.deepcopy the data mapping
    before running this function if you don't want that your provenance information gets
    lost on the original ``data`` mapping.

    Parameters
    ----------
    data : any
        Mapping or values with provenance.

    Returns
    -------
    value : any
        Values in their original format, or lists and dictionaries containing provenance
        values.
    """
    if hasattr(data, "value"):
        assert (
            data == data.value
        ), "The provenance object's value and the original value do not match!"
        return data.value
    elif isinstance(data, list):
        return [clean_provenance(item) for item in data]
    elif isinstance(data, dict):
        return {
            clean_provenance(key): clean_provenance(value)
            for key, value in data.items()
        }
    else:
        return data
