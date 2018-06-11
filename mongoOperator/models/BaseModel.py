# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
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
        for field_name, value in kwargs.items():
            # K8s API returns pascal cased strings, but we use lower-cased strings with underscores instead.
            field_lower = pascal_to_lowercase(field_name)
            field = getattr(type(self), field_lower)
            setattr(self, field_lower, field.parse(value))

    def to_dict(self) -> Dict[str, any]:
        """
        Returns a dictionary with the data of each field.
        :return: A dict with the attribute name as key and the attribute value.
        """
        cls = type(self)
        fields = {attr: getattr(cls, attr) for attr in dir(cls)}
        return {
            attr: field.to_dict(self[attr]) for attr, field in fields.items()
            if isinstance(field, Field) and self[attr] is not None
        }

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
        return getattr(self, attr)

    def __repr__(self) -> str:
        """
        Shows the string-representation of this object.
        :return: The object as string.
        """
        return "{}({})".format(self.__class__.__name__,
                               ", ".join('{}={}'.format(attr, value) for attr, value in self.to_dict().items()))
