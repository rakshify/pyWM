import json
import os

from functools import partial
from typing import Any, Dict, List, Union

import pandas as pd


class FileIO(object):
    @classmethod
    def read(cls, file_path: str, *args, **kwargs) -> Any:
        func_map = {
            "frame": cls._read_frame,
            "json": cls._read_json,
            "text": cls._read_text
        }
        read_using = kwargs.pop('read_using')
        if not read_using and file_path.endswith((".json", ".csv", ".xlsx")):
            read_using = frame
        if read_using not in func_map:
            err = "Reading as %s not supported as of now." % read_using
            raise ValueError(err)
        return func_map[read_using](file_path, *args, **kwargs)

    @classmethod
    def write(cls, obj: Any, file_path: str, *args, **kwargs):
        func_map = {
            "frame": cls._write_frame,
            "json": cls._write_json,
            "text": cls._write_text
        }
        write_using = kwargs.pop('write_using')
        if not write_using and file_path.endswith((".json", ".csv", ".xlsx")):
            write_using = frame
        if write_using not in func_map:
            raise ValueError("Can not write using %s as of now." % write_using)
        return func_map[write_using](obj, file_path, *args, **kwargs)

    @staticmethod
    def _read_text(file_path: str, *args, **kwargs) -> str:
        with open(file_path) as f:
            return f.read()

    @staticmethod
    def _read_json(file_path: str, *args, **kwargs
                   ) -> Union[List[Any], Dict[str, Any]]:
        with open(file_path) as f:
            return json.load(f)

    @staticmethod
    def _read_frame(file_path: str, *args, **kwargs) -> pd.DataFrame:
        read_as = kwargs.pop('read_as', file_path.rsplit(".", 1)[1])
        if read_as == "json":
            foo = partial(pd.read_json, file_path)
        elif read_as == "csv":
            foo = partial(pd.read_csv, file_path)
        elif read_as == "xlsx":
            # sheet_name = kwargs.get("sheet_name", "Sheet1")
            # foo = partial(pd.read_excel, file_path, sheet_name=sheet_name)
            foo = partial(pd.read_excel, file_path)
        else:
            raise ValueError(("Can only read pandas frame from json,"
                              " csv or xlsx file as of now."))
        # fcols = kwargs.get("fcols")
        # if fcols:
        #     foo = partial(foo, names=fcols)
        return foo(**kwargs)

    @staticmethod
    def _write_text(obj: str, file_path: str, *args, **kwargs):
        with open(file_path, "w") as f:
            f.write(obj)

    @staticmethod
    def _write_json(obj: Union[List[Any], Dict[str, Any]], file_path: str,
                    *args, **kwargs):
        with open(file_path, "w") as f:
            json.dump(obj, f)

    @staticmethod
    def _write_frame(obj: pd.DataFrame, file_path: str, *args, **kwargs):
        write_as = kwargs.pop('write_as', file_path.rsplit(".", 1)[1])
        if write_as == "json":
            foo = partial(obj.to_json, file_path,
                          orient="records", index=False)
        elif write_as == "csv":
            foo = partial(obj.to_csv, file_path, index=False)
        elif write_as == "xlsx":
            sheet_name = kwargs.get("sheet_name", "Sheet1")
            with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
                obj.to_excel(writer, sheet_name=sheet_name)
            foo = writer.save
        else:
            raise ValueError(("Can only write pandas frame as json,"
                              " csv or xlsx file as of now."))
        foo(**kwargs)


# IO OPERATIONS
def read_file(*args, **kwargs):
    #
    fp = kwargs.pop("file_path")
    df = FileIO.read(fp, *args, **kwargs)
    return df


def write_file(*args, **kwargs):
    #
    obj = args[0]
    fp = kwargs.pop("file_path")
    FileIO.write(obj, fp, *args, **kwargs)


method_map = {
    "read_file": read_file,
    "write_file": write_file,
    "os_join": lambda *args, **kwargs: os.path.abspath(os.path.join(*args))
}
