# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Dict, Type, Optional

import re


def pascal_to_lowercase(value: str) -> str:
    """
    Converts a string from pascal case into lower-case underscore separated strings.
    e.g. pascalCase => pascal_case.
    Note: K8s API returns pascal cased strings, but in Python we use lower-cased strings with underscores instead.
    :param value: The string to be converted.
    :return: The converted string.
    """
    return re.sub(r"([a-z0-9]+)([A-Z])", r"\1_\2", value).lower()


def lowercase_to_pascal(value: str) -> str:
    """
    Converts a string from lower-case underscore separated strings into pascal case.
    e.g. pascal_case => pascalCase.
    Note: K8s API returns pascal cased strings, but in Python we use lower-cased strings with underscores instead.
    :param value: The string to be converted.
    :return: The converted string.
    """
    return re.sub(r"([a-z0-9]+)_([a-z])", lambda m: m.group(1) + m.group(2).upper(), value)


class Field:
    """
    Base field that can be used in the models. This field does no validation whatsoever.
    """
    def __init__(self, required: bool):
        self.required = required

    def validate(self, value: any) -> None:
        """
        Checks whether the value is valid for this field.
        :param value: The value to be validated.
        :raise ValueError: If the field is invalid.
        """
        if value is None and self.required:
            raise ValueError("The field is required.")

    def parse(self, value: any) -> any:
        """
        Parses the field value.
        :param value: The value to be parsed.
        :return: The parsed value.
        """
        self.validate(value)
        return value

    def to_dict(self, value, skip_validation: bool = False) -> any:
        """
        Returns a the value of this field as it should be set in the model dictionaries.
        :param value: The value to be converted.
        :param skip_validation: Whether the validation should be skipped.
        :return: The value of this field.
        """
        if not skip_validation:
            self.validate(value)
        return value


class StringField(Field):
    """
    Field that converts the given value into a string.
    """
    def parse(self, value: any) -> str:
        return super().parse(str(value))


class EmbeddedField(Field):
    """
    Field that allows sub-models to be created in fields.
    """
    def __init__(self, field_type: Type, required: bool):
        """
        :param field_type: A reference to the type of this field, i.e. the model class.
        :param required: Whether this field is required.
        """
        super().__init__(required)
        self.field_type = field_type

    def parse(self, value: Dict[str, any]):
        if isinstance(value, dict):
            try:
                # K8s API returns pascal cased strings, but we use lower-cased strings with underscores instead.
                values = {pascal_to_lowercase(field_name): field_value for field_name, field_value in value.items()}
                value = self.field_type(**values)
            except TypeError as err:
                raise ValueError("Invalid values passed to {} field: {}. Received {}."
                                 .format(self.field_type.__name__, err, value))
        return super().parse(value)

    def to_dict(self, value, skip_validation: bool = False) -> Optional[Dict[str, any]]:
        value = super().to_dict(value, skip_validation)
        if value is None:
            return None
        return {k: v for k, v in value.to_dict().items() if v is not None}


class MongoReplicaCountField(Field):
    """
    Field that validates that the given value is an integer between 3 and 50. This are the values allowed for the
    amount of MongoDB replicas. It raises a `ValueError` if the validation fails.
    """
    def parse(self, value: int):
        if not isinstance(value, int) or not (3 <= value <= 50):
            raise ValueError("The amount of replica sets must be between 3 and 50 (got {}).".format(repr(value)))
        return super().parse(value)
