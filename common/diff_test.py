from common.diff import DictDiffer


def test_dict_differ_with_change():
    old = { 'academic_term': "2016", "academic_term": "2", "id": 1}
    new = { 'academic_term': "2016", "academic_term": "1"}
    dd = DictDiffer(old, new)
    assert len(dd.changed()) == 1
    assert dd.changed().pop() == 'academic_term'


def test_dict_differ_without_change():
    old = { 'academic_term': "2016", "academic_term": "2", "id": 1}
    new = { 'academic_term': "2016", "academic_term": "2"}
    dd = DictDiffer(old, new)
    assert len(dd.changed()) == 0

