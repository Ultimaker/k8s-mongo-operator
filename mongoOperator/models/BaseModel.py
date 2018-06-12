# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import inspect
import logging

from typing import Dict

from mongoOperator.models.fields import Field, pascal_to_lowercase


class BaseModel:
    """
    Base model within which fields may be defined (using the `Field` class).
    The constructor can then be called to instantiate the values of those fields.
    """
    def __init__(self, **kwargs):
        """
        Creates a new instance of the model.
        :param kwargs: The values of the fields in the model.
        :raise AttributeError: If any of the given attributes did not exist.
        """
        self.fields = dict(inspect.getmembers(type(self), lambda f: isinstance(f, Field)))  # type: Dict[str, Field]

        for field_name, value in kwargs.items():
            # K8s API returns pascal cased strings, but we use lower-cased strings with underscores instead.
            field_lower = pascal_to_lowercase(field_name)
            if field_lower in self.fields:
                setattr(self, field_lower, self.fields[field_lower].parse(value))
            else:
                logging.warning("The model %s does not have the %s field!", type(self), field_lower)

    def validate(self) -> None:
        """
        Checks whether this model is valid.
        """
        for name, field in self.fields.items():
            try:
                field.validate(self[name])
            except (ValueError, AttributeError) as err:
                raise ValueError("Error for field {}: {}".format(repr(name), err))

    def to_dict(self) -> Dict[str, any]:
        """
        Returns a dictionary with the data of each field.
        :return: A dict with the attribute name as key and the attribute value.
        """
        self.validate()
        result = {}
        for name, field in self.fields.items():
            if self[name] is not None:
                result[name] = field.to_dict(self[name])
        return result

    def __eq__(self, other: any) -> bool:
        """
        Checks whether this object is equal to another one.
        :param other: The other object.
        :return: True if they are equal, False otherwise.
        """
        return type(other) == type(self) and other.to_dict() == self.to_dict()

    def __getitem__(self, attr: str) -> any:
        """
        Gets the value of the attribute with the given name.
        :param attr: The attribute name.
        :return: The attribute value.
        """
        value = getattr(self, attr)
        if not isinstance(value, Field):
            return value

    def __repr__(self) -> str:
        """
        Shows the string-representation of this object.
        :return: The object as string.
        """
        return "{}({})".format(self.__class__.__name__,
                               ", ".join('{}={}'.format(attr, value) for attr, value in self.to_dict().items()))
