"""Guards the dependency split between the four Docker targets.

The Dockerfile installs one file per tier from requirements/. If someone adds an
import to services/ and forgets to add the package to the right tier file, the
image still builds and only fails at runtime, in a container, at 3am. These
tests walk the real import graph and fail in CI instead.

They also encode the *absence* side of the split, which is the part a
requirements file cannot express on its own: acquisition must not be able to
reach the database, because that is the property that makes a failed refetch
harmless.
"""

import ast
import re
from pathlib import Path

import pytest

# src/firewatch/tests/test_tiers.py -> parents[3] is the repository root.
PACKAGE_ROOT = Path(__file__).resolve().parents[3]
SRC = PACKAGE_ROOT / "src"
REQUIREMENTS = PACKAGE_ROOT / "requirements"

# Third-party distribution -> importable module names. Distribution and import
# names differ often enough that guessing breaks (psycopg2-binary -> psycopg2,
# python-dotenv -> dotenv, kafka-python -> kafka).
DIST_TO_IMPORT = {
    "fastapi": ["fastapi"],
    "uvicorn": ["uvicorn"],
    "kafka-python": ["kafka"],
    "psycopg2-binary": ["psycopg2"],
    "httpx": ["httpx"],
    "python-dotenv": ["dotenv"],
    # Offline ML only. Listed so a runtime module that imports firewatch.ml
    # fails this test instead of building an image that dies at startup.
    "pandas": ["pandas"],
    "scikit-learn": ["sklearn"],
}

# Entry points per tier, mirroring the Dockerfile CMDs.
TIERS = {
    "acquisition": [
        "firewatch.runner",
        "firewatch.clients.firms",
        "firewatch.clients.weather",
        "firewatch.services.ingestion",
        "firewatch.services.risk",
        "firewatch.config",
    ],
    "processing": [
        "firewatch.runner",
        "firewatch.services.processing",
        "firewatch.services.labels",
        "firewatch.services.risk",
        "firewatch.clients.effis",
        "firewatch.clients.weather",
        "firewatch.config",
    ],
    "api": [
        "firewatch.api.main",
        "firewatch.api.auth",
        "firewatch.api.auth_routes",
        "firewatch.api.endpoints",
        "firewatch.config",
    ],
    "forecast": [
        "firewatch.runner",
        "firewatch.clients.daily_weather",
        "firewatch.ml.forecast",
        "firewatch.ml.features",
        "firewatch.services.forecast",
        "firewatch.config",
    ],
}

ALL_THIRD_PARTY = {m for mods in DIST_TO_IMPORT.values() for m in mods}


def _module_path(module: str) -> Path | None:
    parts = module.split(".")
    candidate = SRC.joinpath(*parts).with_suffix(".py")
    if candidate.exists():
        return candidate
    package = SRC.joinpath(*parts) / "__init__.py"
    return package if package.exists() else None


def _imports_of(module: str) -> set[str]:
    """Third-party modules imported by `module`, transitively within firewatch.

    Function-local imports are deliberately EXCLUDED. A deferred import is a
    runtime choice made on purpose: `runner daily` (acquisition) lives in the
    same module as `geocode-backfill` (processing), and the deferred import is
    what keeps psycopg2 out of the acquisition image. Counting it as a tier
    dependency would make the guard reject exactly the mechanism that makes the
    split work.

    Only module-level imports bind when the module is imported, so only those
    can make an image fail at import time.
    """
    path = _module_path(module)
    assert path is not None, f"module not found: {module}"
    tree = ast.parse(path.read_text(encoding="utf-8"))

    # Nodes belonging to a nested scope (function/lambda/class body). Imports
    # there are executed on call, not on import.
    deferred: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            deferred.update(id(child) for child in ast.walk(node))
        elif isinstance(node, ast.ClassDef):
            # Class bodies DO run at import time, so only skip function bodies
            # nested inside them; a direct method body is still deferred.
            deferred.update(
                id(child)
                for child in ast.walk(node)
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            )
            for sub in node.body:
                deferred.update(id(child) for child in ast.walk(sub) if sub is not node)

    local: set[str] = set()
    external: set[str] = set()
    for node in ast.walk(tree):
        if id(node) in deferred:
            continue
        if isinstance(node, ast.Import):
            for alias in node.names:
                (local if alias.name.startswith("firewatch") else external).add(
                    alias.name.split(".")[0]
                )
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # relative import, stays inside the package
                parts = module.split(".")[:-node.level]
                if node.module:
                    parts = parts + node.module.split(".")
                local.add(".".join(parts))
            elif node.module:
                (local if node.module.startswith("firewatch") else external).add(
                    node.module.split(".")[0]
                )

    found = external & ALL_THIRD_PARTY
    for dep in local - {"firewatch"}:
        if dep not in module:
            found |= _imports_of(dep)
    return found


def _tier_requirements(tier: str) -> dict[str, str]:
    """Read requirements/<tier>.txt, following `-r` includes."""
    resolved: dict[str, str] = {}
    pending = [(REQUIREMENTS / f"{tier}.txt").resolve()]
    seen: set[Path] = set()
    while pending:
        path = pending.pop()
        if path in seen or not path.exists():
            continue
        seen.add(path)
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("-r "):
                pending.append((path.parent / line[3:].strip()).resolve())
                continue
            if "==" in line:
                name, version = line.split("==", 1)
                resolved[name.strip()] = version.strip()
    return resolved


@pytest.mark.parametrize("tier", sorted(TIERS))
def test_tier_requirements_cover_imports(tier):
    """Every third-party module a tier imports must be installed in that image."""
    declared = _tier_requirements(tier)
    provided_modules = {
        m for dist, mods in DIST_TO_IMPORT.items() if dist in declared for m in mods
    }

    needed: set[str] = set()
    for entry in TIERS[tier]:
        needed |= _imports_of(entry)

    missing = needed - provided_modules
    assert not missing, (
        f"tier '{tier}' imports {sorted(missing)} but requirements/{tier}.txt does not "
        f"provide them. The image would build and then fail at runtime. Add the "
        f"distribution to requirements/{tier}.txt (and to base.txt if a shared "
        f"tier needs it)."
    )


def test_requirements_aggregate_is_a_superset():
    """requirements.txt must stay a superset so local installs keep working."""
    full = _tier_requirements_file(PACKAGE_ROOT / "requirements.txt")
    api = _tier_requirements("api")
    assert set(api) <= set(full), (
        f"requirements.txt is missing {sorted(set(api) - set(full))}; a host-side "
        f"`pip install -r requirements.txt` would then fail to run the app"
    )


def _tier_requirements_file(path: Path) -> dict[str, str]:
    """Resolve a requirements file located anywhere in the tree."""
    resolved: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-r "):
            resolved |= _tier_requirements_file((path.parent / line[3:].strip()).resolve())
        elif "==" in line:
            name, version = line.split("==", 1)
            resolved[name.strip()] = version.strip()
    return resolved


def test_acquisition_cannot_reach_the_database():
    """The acquisition tier must have no database driver installed.

    This is the property the split is actually for. A refetch that crashes
    should be structurally unable to write to PostGIS, not merely unlikely to.
    """
    declared = _tier_requirements("acquisition")
    assert "psycopg2-binary" not in declared, (
        "acquisition must not ship psycopg2-binary: the tier that fetches "
        "external data has no business writing to the database"
    )
    for entry in TIERS["acquisition"]:
        imports = _imports_of(entry)
        assert "psycopg2" not in imports, (
            f"{entry} imports psycopg2, which acquisition.txt does not provide; "
            f"either drop the import or move the module to the processing tier"
        )


def test_processing_does_not_ship_a_web_framework():
    """Nothing in the processing tier serves HTTP, so it needs no web stack."""
    declared = _tier_requirements("processing")
    for dist in ("fastapi", "uvicorn"):
        assert dist not in declared, f"processing must not depend on {dist}"
    for entry in TIERS["processing"]:
        imports = _imports_of(entry)
        assert not imports & {"fastapi", "uvicorn", "starlette"}, (
            f"{entry} imports a web framework; processing serves no HTTP"
        )


def test_forecast_tier_has_ml_and_database_dependencies():
    """The worker loads the joblib model and writes fire_predictions."""
    declared = _tier_requirements("forecast")
    assert {"pandas", "scikit-learn", "psycopg2-binary"} <= set(declared)


def test_forecast_does_not_ship_a_web_framework():
    """Inference is a worker, not another HTTP surface."""
    declared = _tier_requirements("forecast")
    for dist in ("fastapi", "uvicorn"):
        assert dist not in declared, f"forecast must not depend on {dist}"
    for entry in TIERS["forecast"]:
        imports = _imports_of(entry)
        assert not imports & {"fastapi", "uvicorn", "starlette"}, (
            f"{entry} imports a web framework; forecast serves no HTTP"
        )


def test_forecast_worker_is_wired_to_run_continuously():
    """A one-shot command in compose would leave the map stale, not auto-updated."""
    dockerfile = (PACKAGE_ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = (PACKAGE_ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert re.search(
        r'CMD \["python", "-m", "firewatch\.runner", "forecast", "--loop"',
        dockerfile,
    )
    assert re.search(r"^  forecast:\s*$", compose, re.M)
    assert re.search(r"^\s*- --loop\s*$", compose, re.M)
    assert "./data/models:/app/data/models:ro" in compose


def test_update_intervals_are_configurable():
    """Both workers must read their cadence from .env, not a hardcoded CMD."""
    compose = (PACKAGE_ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "${REFRESH_INTERVAL_MINUTES:-30}" in compose
    assert "${FORECAST_INTERVAL_MINUTES:-180}" in compose


def test_dockerfile_targets_match_tiers():
    """Every tier file must have a matching `--target` in the Dockerfile."""
    dockerfile = (PACKAGE_ROOT / "Dockerfile").read_text(encoding="utf-8")
    targets = set(re.findall(r"^FROM\s+\S+\s+AS\s+(\w+)", dockerfile, re.M))
    for tier in TIERS:
        assert tier in targets, f"Dockerfile has no 'AS {tier}' stage"


def test_compose_build_targets_are_explicit():
    """A service without `build.target` would silently get the wrong image."""
    compose = (PACKAGE_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    declared_targets = set(re.findall(r"^\s+target:\s*(\w+)\s*$", compose, re.M))
    assert declared_targets == set(TIERS), (
        f"compose build targets {sorted(declared_targets)} do not match the "
        f"tier files {sorted(TIERS)}"
    )


def test_each_tier_has_a_distinct_image_tag():
    """A shared tag hides version drift between running containers."""
    compose = (PACKAGE_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    tags = re.findall(r"^\s{4}image:\s*firewatch:(\S+)", compose, re.M)
    app_tags = [t for t in tags if t != "local"]
    assert len(app_tags) == len(set(app_tags)) == len(TIERS), (
        f"expected {len(TIERS)} distinct firewatch:* tags, found {app_tags}"
    )
