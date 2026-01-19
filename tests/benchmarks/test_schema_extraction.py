"""Benchmark tests for schema extraction performance."""
import pytest


@pytest.mark.benchmark
def test_get_dict_keys_performance(benchmark):
    """Benchmark get_dict_keys function with large dataset."""
    from undatum.utils import get_dict_keys

    # Create sample data with many nested keys
    def generate_data():
        for i in range(1000):
            yield {
                'field1': f'value{i}',
                'nested': {
                    'subfield1': f'subvalue{i}',
                    'subfield2': {
                        'deepfield': f'deepvalue{i}'
                    }
                },
                'list_field': [{'item': f'item{i}'}]
            }

    data = list(generate_data())
    result = benchmark(get_dict_keys, data, limit=1000)
    assert len(result) > 0


@pytest.mark.benchmark
def test_dict_generator_performance(benchmark):
    """Benchmark dict_generator function."""
    from undatum.utils import dict_generator

    sample_dict = {
        'field1': 'value1',
        'nested': {
            'subfield1': 'subvalue1',
            'subfield2': {
                'deepfield': 'deepvalue1'
            }
        },
        'list_field': [{'item': 'item1'}, {'item': 'item2'}]
    }

    def run_generator():
        return list(dict_generator(sample_dict))

    result = benchmark(run_generator)
    assert len(result) > 0
