"""Testes da CLI -- especialmente a normalizacao de argumentos vinda do job."""

from nyc_taxi.run import parse_args


def test_meses_como_argumentos_separados():
    args = parse_args(["--layer", "silver", "--months", "2024-01", "2024-02"])
    assert args.months == ["2024-01", "2024-02"]


def test_meses_como_string_unica_do_job_databricks():
    """O Databricks Job passa {{job.parameters.months}} como um unico argumento."""
    args = parse_args(["--layer", "gold", "--months", "2024-01 2024-02 2024-03"])
    assert args.months == ["2024-01", "2024-02", "2024-03"]


def test_defaults():
    args = parse_args([])
    assert args.layer == "all"
    assert args.months is None
