"""Planning evidence must cover every variant without hiding differences in filter scope."""
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conversations import CASES, normalized_scope
from bi_analyst.metrics.compiler import Compiler


def test_live_benchmark_covers_every_registered_metric_variant():
    assert {case[2] for case in CASES if case[2]} == set(Compiler().metrics)


@pytest.mark.parametrize('filters', [[], [{'dimension':'type', 'operator':'in', 'values':['New Build']}]])
def test_comparison_normalization_accepts_identical_inherited_and_explicit_filters(filters):
    inherited = {'period':'previous_month', 'filters':None}
    explicit = {'period':'previous_month', 'filters':filters}
    assert normalized_scope('comparison', inherited, filters) == normalized_scope('comparison', explicit, filters)


def test_comparison_normalization_preserves_an_explicit_unfiltered_baseline():
    current = [{'dimension':'type', 'operator':'in', 'values':['New Build']}]
    inherited = {'period':'previous_month', 'filters':None}
    unfiltered = {'period':'previous_month', 'filters':[]}
    assert normalized_scope('comparison', inherited, current) != normalized_scope('comparison', unfiltered, current)


def test_filter_order_is_irrelevant_but_values_are_preserved():
    a = [{'dimension':'type', 'operator':'in', 'values':['New Build', 'Refurbishment']},
         {'dimension':'category', 'operator':'is_null', 'values':[]}]
    b = [a[1], {**a[0], 'values':['Refurbishment', 'New Build']}]
    assert normalized_scope('filters', a, []) == normalized_scope('filters', b, [])
    assert normalized_scope('filters', a, []) != normalized_scope('filters', a[:1], [])
