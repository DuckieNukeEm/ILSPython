import pytest
import ILSPython
from ILSPython import IowaLiquorSalesAPI


def test_init():
    API = IowaLiquorSalesAPI()
    assert API


def test_query_sales():
    API = IowaLiquorSalesAPI()
    res = API.query_sales()
    assert len(res) > 0
    res = API.query_sales(params={"date": ["2022-01-04", "2022-01-05"]})
    assert len(res) > 0
    assert len(API.results) == 0
    API.query_sales(
        params={"date": ["2022-01-04", "2022-01-05"]},
        return_results=False,
    )
    assert len(API.results) > 0


def test_get_saved_results():
    API = IowaLiquorSalesAPI()
    assert len(API.results) == 0
    res = API.query_sales(return_results=False)
    size_of = len(API.results)
    assert res is None
    assert size_of > 0
    res = API.get_saved_results()
    assert len(res) > 0
    assert len(res) == size_of
