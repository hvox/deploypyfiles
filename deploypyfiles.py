import sys
from ast import literal_eval
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import chain
from json import dumps
from pathlib import Path
from shutil import rmtree
from subprocess import run
from tomllib import loads
from typing import Any


def main(root_dir: Path | None = None) -> int:
    config_path = find_file(root_dir or Path(), "pyproject.toml")
    if config_path is None:
        raise ValueError(f"pyproject.toml not found")
    root = config_path.parent
    config: dict[str, Any] = loads(config_path.read_text("utf-8", "strict"))
    config = config["tool"]["deploy"]
    test_command = config.get("test")
    if test_command and (errors := run_tests(root, test_command)):
        eprint("Some tests failed:\n" + "\n".join(errors))
        if input("Proceed with deployment? [y/N] ").lower().strip() != "y":
            return 1
    sources = get_sources(root, config)
    targets = get_targets(root, config)
    template = root / config["template"] if "template" in config else None
    archive = config.get("archive")
    for target in targets:
        print(f"Deploying to {target}")
        resolved = target.resolve()
        if str(resolved) != str(target):
            print(9 * " " + f"AKA {resolved}")
        for destination, source in sources.items():
            deploy(root, source, resolved / destination, template, archive)
    return 0


@dataclass
class Config:
    root: Path
    src: Path

    preship: list[list[str]]
    sources: list[Path]
    targets: list[Path]
    archive: list[Path]

    @staticmethod
    def from_config(root: Path, config: dict[str, Any]) -> Config:
        src_dir = root / "src" if "src" in root.iterdir() else root
        preship = [
            list(map(str, cmd)) if isinstance(cmd, list) else str(cmd).split()
            for cmd in config.get("prerequisites", [])
        ]
        sources = [Path(str(path)) for path in config.get("deployables", [])]
        targets = [Path(str(path)) for path in config.get("destinations", [".."])]
        archive = [Path(str(path)) for path in config.get("archive", [])]
        for key in config:
            if key not in {"prerequisites", "deployables", "destinations", "archives"}:
                eprint(f"Encountered unsupported key {key!r} in pyproject.toml")
        return Config(root, src_dir, preship, sources, targets, archive)

    def to_toml(self) -> str:
        config = [
            "prerequisites = " + tomlify(self.preship),
            "deployables = " + tomlify(self.sources),
            "destinations = " + tomlify(self.targets),
            "archive = " + tomlify(self.archive),
        ]
        return "\n".join(config)


def run_tests(root: Path, tests: str | list[str]) -> list[str]:
    errors = []
    for test in [tests] if isinstance(tests, str) else tests:
        cmd = test.split()
        if cmd[0].endswith(".py"):
            cmd = ["python"] + cmd
        print("> " + " ".join(cmd))
        if run(cmd, cwd=root, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr).returncode:
            errors.append("> " + " ".join(cmd))
    return errors


def get_sources(root: Path, config: dict[str, Any]) -> dict[str, Path]:
    sources_dir = root / "src" if "src" in root.iterdir() else root
    paths: dict[str, Path] = find_sources(sources_dir)
    if "source" in config:
        path: Path = root / config["source"]
        paths[path.stem] = path
    return paths


def find_sources(path: Path) -> dict[str, Path]:
    if path.is_dir(follow_symlinks=False):
        paths = {}
        for path in iterdir(path):
            paths.update(find_sources(path))
        return paths
    if not path.is_file(follow_symlinks=False):
        return {}
    lines = path.read_text("utf-8", "ignore").splitlines()
    if path.suffix == ".py" or lines[0].startswith("#!/usr/bin/env python"):
        for line in lines[:5]:
            if not line.startswith("DEPLOY_TARGET = "):
                continue
            deploy_target = literal_eval(line.split(" = ", 1)[1])
            return {deploy_target: path}
    return {}


def get_targets(root: Path, config: dict[str, Any]) -> list[Path]:
    targets: list[Path] = []
    if main_target := config.get("target"):
        targets.append(Path(main_target))
    targets.extend(map(Path, config.get("targets", [])))
    return [path if path.is_absolute() else Path(root, path) for path in targets]


def deploy(
    root: Path,
    source_path: Path,
    target_root: Path,
    template: Path | None = None,
    archive: str | None = None,
) -> None:
    print(">", source_path.relative_to(root.parent), "->", target_root)
    source_root = source_path.parent
    source_paths = [source_path] + get_dependencies(source_path)
    targets = {target_root / path.relative_to(source_root): path for path in source_paths}
    targets = {
        t.with_stem(target_root.stem) if t.stem == source_path.stem else t: s
        for t, s in targets.items()
    }
    if isinstance(template, Path):
        for path in iterdir(template):
            target_path = target_root / path.relative_to(template)
            if path.stem == "TARGET":
                target_path = target_path.with_stem(target_root.stem)
            elif path.name == "requirements.txt" and Path(root, "requirements.txt").is_file():
                path = Path(root, "requirements.txt")
            if target_path not in targets:
                targets[target_path] = path
    updated = False
    for target_path, source_path in targets.items():
        print(" ", source_path.relative_to(root.parent), "->", target_path)
        if target_path.is_file() and target_path.read_bytes() == source_path.read_bytes():
            continue
        target_path.write_bytes(source_path.read_bytes())
        updated = True
    if not archive or not updated:
        return
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    archive_path = target_root / archive / today
    print(" ", f"Archived to {archive_path}")
    rmtree(archive_path, ignore_errors=True)
    archive_path.mkdir(exist_ok=True, parents=True)
    for target_path, source_path in targets.items():
        target_path = archive_path / target_path.relative_to(target_root)
        target_path.write_bytes(source_path.read_bytes())


def get_dependencies(path: Path) -> list[Path]:
    deps = {}
    queue = [path]
    for path in queue:
        for line in path.read_text("utf-8", "strict").splitlines():
            line = line.strip()
            if not line.startswith("import ") and not line.startswith("from "):
                continue
            mod_name = line.split()[1]
            mod_path = path.parent / f"{mod_name}.py"
            if mod_path.is_file() and mod_path not in deps:
                deps[mod_path] = mod_name
                queue.append(mod_path)
    return list(deps.keys())


def find_file(path: Path, name: str) -> Path | None:
    path = path.resolve()
    for path in chain([path], path.parents):
        path = Path(path, name)
        if path.exists():
            return path
    return None


def tomlify(obj: object) -> str:
    match obj:
        case list(elements):
            return "[" + ", ".join(map(tomlify, elements)) + "]"
        case Path(path):
            string = str(path)
            return f"'{string}'" if "'" not in string else tomlify(string)
        case str(string):
            return dumps(string, ensure_ascii=False)
        case int(number) | float(number):
            return str(number)
    raise NotImplemented(f"tomlify({obj!r})")


def iterdir(path: Path) -> list[Path]:
    paths = [path for path in path.iterdir() if path.stem[:1] not in "._: "]
    paths.sort(key=str)
    return paths


def eprint(*values: object) -> None:
    message = " ".join(map(str, values))
    if sys.stderr.isatty():
        message = f"\x1b[91m{message}\x1b[0m"
    print(message, file=sys.stderr, flush=True)


if __name__ == "__main__":
    sys.exit(main())
