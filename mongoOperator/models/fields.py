# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-

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


class Field:
    """
    Base field that can be used in the models. This field does no validation whatsoever.
    """
    def parse(self, value: any) -> any:
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
    def __init__(self, field_type):
        self.field_type = field_type

    def parse(self, value):
        if isinstance(value, dict):
            # K8s API returns pascal cased strings, but we use lower-cased strings with underscores instead.
            values = {pascal_to_lowercase(field_name): field_value
                      for field_name, field_value in value.items()}
            value = self.field_type(**values)
        return super().parse(value)


class MongoReplicaCountField(Field):
    """
    Field that validates that the given value is an integer between 3 and 50. This are the values allowed for the
    amount of MongoDB replicas. It raises a `ValueError` if the validation fails.
    """
    def parse(self, value: int):
        if not isinstance(value, int) or not (3 <= value <= 50):
            raise ValueError("The amount of replica sets must be between 3 and 50 (got {}).".format(repr(value)))
        return super().parse(value)
