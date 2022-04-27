from __future__ import annotations


def define_access_level(pii: str, private: str) -> str:
    if pii == "true" and private == "true":
        return "full"

    if pii == "true":
        return "sensitive"

    if private == "true":
        return "restrict"

    return "partial"


def build_default_boolean(index: int | None, _list: list, field_type: str) -> bool:

    if index is None:
        return False

    if field_type in _list[index]:
        return _list[index][field_type]
    return False


def build_data_classification(index: int | None, _list: list) -> str:

    if index is None:
        return ""

    if "data_classification" in _list[index]:
        return _list[index]["data_classification"]
    return ""


def build_classifications(index: int | None, _list: list) -> dict:

    pii = str(build_default_boolean(
        index=index, _list=_list, field_type="pii")).lower()
    private = str(build_default_boolean(
        index=index, _list=_list, field_type="private")).lower()
    access_level = define_access_level(pii, private)
    manipulated = str(build_default_boolean(
        index=index, _list=_list, field_type="manipulated")).lower()
    data_classification = build_data_classification(index=index, _list=_list)

    return {
        "pii": pii,
        "private": private,
        "access_level": access_level,
        "manipulated": manipulated,
        "data_classification": data_classification
    }
