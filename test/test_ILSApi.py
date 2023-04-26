import pytest
import ILSApi as IL


def test_init():
    API = IL.IowaLiquorSalesAPI()
    assert API


def test_query_sales():
    API = IL.IowaLiquorSalesAPI()
    res = API.query_sales()
    assert len(res) > 0
    res = API.query_sales(
        params={"date": ["2022-01-01", "2022-01-02"], "city": "Adair"}
    )
    assert len(res) > 0
    assert len(API.results)
    API.query_sales(
        params={"date": ["2022-01-01", "2022-01-02"], "city": "Adair"},
        return_results=False,
    )
    assert len(API.results)


def test_get_saved_results():
    API = IL.IowaLiquorSalesAPI()
    assert len(API.results) == 0
    res = API.query_sales(return_results=False)
    size_of = len(API.results)
    assert len(res) is None
    assert size_of > 0
    res - API.get_saved_results()
    assert res > 0
    assert len(res) == size_of
