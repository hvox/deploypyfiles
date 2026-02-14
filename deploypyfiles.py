import sys
from ast import literal_eval
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import chain
from json import dumps
from pathlib import Path
from shutil import rmtree
from string import printable
from subprocess import run
from tomllib import loads
from typing import Any, Iterable, Iterator

REPORT_CONFIG = True
PACKAGE_NAME = "deploypyfiles"
PYTHON_SHEBANGS = ["#!/usr/bin/env python"]
IDENTIFIER_CHARS = printable[:62] + "_"


def main(*opts: str) -> int:
    assert not opts
    config_path = find_file(Path(), "pyproject.toml")
    if config_path is None:
        raise ValueError(f"pyproject.toml not found")
    root = config_path.parent
    project_config = loads(config_path.read_text("utf-8", "strict"))
    config = Config.from_dict(root, subdict(project_config, "tool", PACKAGE_NAME))
    if REPORT_CONFIG:
        tprint(f"Using config file {config_path}")
        print(f"{config.to_toml()}\n")
    else:
        print(f"Using config file {config_path}")
    if errors := run_tests(config.root, config.preship):
        eprint("Some tests failed:\n" + "\n".join(errors))
        if input("Proceed with deployment? [y/N] ").lower().strip() != "y":
            return 1
    success = deploy(config)
    return 0 if success else 1


def deploy(prj: Config) -> None:
    tprint("# Deploying time!")
    if not prj.targets:
        eprint("No destinations specified, nowhere to deploy :(")
    for destination_root in prj.targets:
        print(f"Deploying to {destination_root}")
        resolved_destination = destination_root.resolve()
        if str(resolved_destination) != str(destination_root):
            print(f"which is aka {resolved_destination}")
            destination_root = resolved_destination
        for source, destination in find_deployables(prj.root / path for path in prj.sources):
            destination = destination_root / destination
            deploy_file(prj, source, destination)
    return None


def deploy_file(prj: Config, main_path: Path, destination: Path) -> bool:
    print(">", main_path.relative_to(prj.root.parent), "->", destination)
    source_root = main_path.parent
    if destination.is_dir():
        destination_root = destination
        main_destination = destination_root / main_path.name
    elif destination.parent.is_dir():
        destination_root = destination.parent
        main_destination = destination
    else:
        eprint(f"[FAILURE] Failed to find path {destination}")
        return False
    mapping: dict[Path, Path] = {}
    for source in chain([main_path], get_dependencies(main_path)):
        dest = destination_root / source.relative_to(source_root)
        if source.stem == main_path.stem:
            dest = dest.with_stem(main_destination.stem)
        if dest in mapping:
            eprint(
                "[FAILURE] Path collision:\n ",
                f"{mapping[dest].relative_to(prj.root.parent)} -> {dest}\n ",
                f"{source.relative_to(prj.root.parent)} -> {dest}",
            )
            return False
        mapping[dest] = source
    if template := prj.templates.get("DEFAULT"):
        template_root = template.resolve()
        for source in iterdir(template_root):
            dest = destination_root / source.relative_to(template_root)
            if source.stem == main_path.stem:
                dest = dest.with_stem(main_destination.stem)
            if dest not in mapping:
                mapping[dest] = source
    anything_updated = False
    for dest, source in mapping.items():
        if dest.is_file():
            if dest.read_bytes() == source.read_bytes():
                action = None
            else:
                action = "update"
        elif dest.exists():
            eprint(f"  [FAILURE] Path already taken: {dest}")
            action = "error"
        else:
            action = "new"
        if action == "new" or action == "update":
            dest.write_bytes(source.read_bytes())
            anything_updated = True
        sign = {"new": "+", "update": "u", "error": "?", None: " "}[action]
        message = " ".join(map(str, [sign, source.relative_to(prj.root.parent), "->", dest]))
        if sign in "+u":
            gprint(message)
        elif sign in "?":
            eprint(message)
        else:
            print(message)
    if not prj.archive or not anything_updated:
        return True
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for archive in prj.archive:
        archive_path = destination_root / archive / today
        rmtree(archive_path, ignore_errors=True)
        archive_path.mkdir(exist_ok=True, parents=True)
        for dest, source in mapping.items():
            dest = archive_path / dest.relative_to(destination_root)
            dest.write_bytes(source.read_bytes())
        gprint(" ", f"Archived to {archive_path}")
    return True


def find_deployables(
    paths: Iterable[Path], guess_destination: bool = True
) -> Iterator[tuple[Path, Path]]:
    for path in paths:
        if path.is_dir():
            yield from find_deployables(
                (path for path in path.iterdir() if path.name[0] not in "._"),
                guess_destination=False,
            )
            continue
        elif not path.is_file():
            continue
        lines = path.read_text("utf8", "ignore").splitlines()
        if path.suffix != ".py" and not any(
            lines[0].startswith(shebang) for shebang in PYTHON_SHEBANGS
        ):
            continue
        destination = None
        for line in lines[:5]:
            if line.startswith("DEPLOY_TARGET = ") or line.startswith("DEPLOYMENT_DESTINATION = "):
                destination = literal_eval(line.split(" = ", 1)[1])
                break
        if destination is not None:
            yield (path, Path(destination))
        elif guess_destination:
            yield (path, path.relative_to(path.parent))


@dataclass
class Config:
    root: Path
    src: Path

    templates: dict[str, Path]
    preship: list[list[str]]
    sources: list[Path]
    targets: list[Path]
    archive: list[Path]

    @staticmethod
    def from_dict(root: Path, config: dict[str, Any]) -> Config:
        src_dir = root / "src" if (root / "src").is_file() else root
        templs = {name: Path(str(path)) for name, path in subdict(config, "templates").items()}
        preship = [parse_command(cmd) for cmd in config.get("prerequisites", [])]
        sources = [src_dir.relative_to(root)]
        sources = [Path(str(path)) for path in config.get("deployables", sources)]
        targets = [Path(str(path)) for path in config.get("destinations", [".."])]
        archive = [Path(str(path)) for path in config.get("archives", [])]
        for key in config:
            if key not in {"templates", "prerequisites", "deployables", "destinations", "archives"}:
                eprint(f"Encountered unsupported key {key!r} in pyproject.toml")
        return Config(root, src_dir, templs, preship, sources, targets, archive)

    def to_toml(self) -> str:
        # TODO: fields
        config = [
            "templates = " + tomlify(self.templates),
            "prerequisites = " + tomlify(self.preship),
            "deployables = " + tomlify(self.sources),
            "destinations = " + tomlify(self.targets),
            "archive = " + tomlify(self.archive),
        ]
        return "\n".join(config)


# TODO
# @dataclass (SourceFile): destinaton...
# and in TOML: sources = ["path1", {source="path2", destination="path3"}]
# In source we expect DEPLOYMENT_DESTIONATION = "some-path"


def run_tests(root: Path, tests: str | list[str] | list[list[str]]) -> list[str]:
    errors = []
    for test in [tests] if isinstance(tests, str) else tests:
        cmd = test.split() if isinstance(test, str) else test
        if cmd[0].endswith(".py"):
            cmd = ["python"] + cmd
        tprint("> " + " ".join(cmd))
        if run(cmd, cwd=root, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr).returncode:
            errors.append("> " + " ".join(cmd))
    return errors


def deploy_script(
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
    queue = [path]
    deps: dict[Path, str] = {}
    if path.suffixes == [".py"]:
        stubfile = path.with_suffix(".pyi")
        if stubfile.exists():
            deps[stubfile] = path.stem
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
        case dict(mapping):
            elements: list[str] = []
            for key, value in mapping.items():
                assert isinstance(key, str) and all(ch in IDENTIFIER_CHARS for ch in key)
                elements.append(f"{key} = {tomlify(value)}")
            return "{" + ", ".join(elements) + "}"
        case path if isinstance(path, Path):
            string = str(path)
            return f"'{string}'" if "'" not in string else tomlify(string)
        case str(string):
            return dumps(string, ensure_ascii=False)
        case int(number) | float(number):
            return str(number)
        case obj:
            raise NotImplementedError(f"tomlify({obj!r})")


def subdict(dictionary: dict[str, Any], *keys: str) -> dict[str, Any]:
    obj: object = dictionary
    for key in keys:
        obj = obj.get(key) if isinstance(obj, dict) else None
    if isinstance(obj, dict) and all(isinstance(k, str) for k in obj):
        return obj
    return {}


def parse_command(command: str | list[str]) -> list[str]:
    if isinstance(command, str):
        return command.split()
    return command


def iterdir(path: Path) -> list[Path]:
    paths = [path for path in path.iterdir() if path.stem[:1] not in "._: "]
    paths.sort(key=str)
    return paths


def tprint(*values: object) -> None:
    message = " ".join(map(str, values))
    if sys.stdout.isatty():
        message = f"\x1b[94m{message}\x1b[0m"
    print(message, file=sys.stdout, flush=True)


def gprint(*values: object) -> None:
    message = " ".join(map(str, values))
    if sys.stdout.isatty():
        message = f"\x1b[92m{message}\x1b[0m"
    print(message, file=sys.stdout, flush=True)


def eprint(*values: object) -> None:
    message = " ".join(map(str, values))
    if sys.stderr.isatty():
        message = f"\x1b[91m{message}\x1b[0m"
    print(message, file=sys.stderr, flush=True)


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
