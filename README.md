<!-- Bump 2026-03-09 -->
# fuel-price-analysis

Analysis on fuel prices across different areas, fuels and categories

## Requirements

- macOS/Linux: `curl -LsSf https://astral.sh/uv/install.sh | sh`
or
- Windows: `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`

uv can manage Python versions directly. To install Python 3.13 via uv: `uv python install 3.13`

## Installation

```sh
uv sync
```

After this step you may want to close and reopen your terminal or IDE to ensure that the uv-managed virtual environment is activated correctly.

## Tests

```sh
uv run poe test
```

## Linting

```sh
uv run poe lint
```

## Typechecking

```sh
uv run poe typecheck
```
