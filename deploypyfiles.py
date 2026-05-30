import sys
from ast import literal_eval
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import chain
from json import dumps
from pathlib import Path
from shutil import rmtree
from string import printable
from subprocess import run
from tomllib import loads
from typing import Any

REPORT_CONFIG = True
PACKAGE_NAME = "deploypyfiles"
PYTHON_SHEBANGS = ["#!/usr/bin/env python"]
IDENTIFIER_CHARS = printable[:62] + "_"
DESTINATION_MARKER = "# DESTINATION: "


def main(*opts: str) -> int:
    assert not opts
    config_path = find_file(Path(), "pyproject.toml")
    if config_path is None:
        raise ValueError("pyproject.toml not found")
    root = config_path.parent
    project_config = loads(config_path.read_text("utf-8", "strict"))
    config = Config.from_dict(root, subdict(project_config, "tool", PACKAGE_NAME))
    if REPORT_CONFIG:
        tprint(f"Using config file {config_path}")
        print(f"[tool.{PACKAGE_NAME}]")
        print(f"{config.to_toml()}\n")
    else:
        print(f"Using config file {config_path}")
    if errors := run_tests(config.root, config.preship):
        eprint("Some tests failed:\n" + "\n".join(errors))
        if input("Proceed with deployment? [y/N] ").lower().strip() != "y":
            return 1
    success = deploy(config)
    return 0 if success else 1


def deploy(prj: Config) -> bool:
    tprint("# Deploying time!")
    if not prj.targets:
        eprint("No destinations specified, I don't know where to deploy >.<")
    success = True
    for destination_root in prj.targets:
        if not destination_root.is_absolute():
            destination_root = prj.root / destination_root
        print(f"Deploying to {destination_root}")
        resolved_destination = destination_root.resolve()
        if str(resolved_destination) != str(destination_root):
            print(f"which is aka {resolved_destination}")
            destination_root = resolved_destination
        for source, destination in find_deployables(prj, (prj.root / path for path in prj.sources)):
            destination = destination_root / destination
            success |= deploy_file(prj, source, destination)
    return success


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
        template_root = Path(prj.root, template).resolve()
        for source in iterdir(template_root):
            dest = destination_root / source.relative_to(template_root)
            if source.stem in (main_path.stem, "FILESTEM"):
                dest = dest.with_stem(main_destination.stem)
            if dest not in mapping:
                mapping[dest] = source
    anything_updated = copy_files(prj, destination_root, mapping)
    if not prj.archive or not anything_updated:
        return True
    archive_files(prj, destination_root, mapping)
    return True


def copy_files(config: Config, destination_root: Path, mapping: dict[Path, Path]) -> bool:
    anything_updated = False
    for dest, source in mapping.items():
        if dest.is_file():
            if dest.read_bytes() == read_file(config, source):
                action = None
            else:
                action = "update"
        elif dest.exists():
            eprint(f"  [FAILURE] Path already taken: {dest}")
            action = "error"
        else:
            action = "new"
        if action in ("new", "update"):
            if action == "new":
                dest.parent.mkdir(parents=True, exist_ok=True)
            elif action == "update":
                backup_file(config, dest)
            dest.write_bytes(read_file(config, source))
            anything_updated = True
        sign = {"new": "+", "update": "u", "error": "?", None: " "}[action]
        message = " ".join(map(str, [sign, source.relative_to(config.root.parent), "->", dest]))
        if sign in "+u":
            gprint(message)
        elif sign in "?":
            eprint(message)
        else:
            print(message)
    return anything_updated


def backup_file(config: Config, path: Path) -> None:
    backup_counter = int(getattr(backup_file, "counter", 1))
    # prefix = hex(hash(str(path)))[2:] + "_"
    prefix = f"{backup_counter:03X}_"
    for backup_dir in config.backup_dirs:
        (backup_dir / f"{prefix} {path.name}").write_bytes(path.read_bytes())
    setattr(backup_file, "counter", backup_counter + 1)


def archive_files(config: Config, destination_root: Path, mapping: dict[Path, Path]) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for archive in config.archive:
        archive_path = destination_root / archive / today
        rmtree(archive_path, ignore_errors=True)
        archive_path.mkdir(exist_ok=True, parents=True)
        for dest, source in mapping.items():
            dest = archive_path / dest.relative_to(destination_root)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(read_file(config, source))
        gprint(" ", f"Archived to {archive_path}")


def find_deployables(
    config: Config, paths: Iterable[Path], guess_destination: bool = True
) -> Iterator[tuple[Path, Path]]:
    for path in paths:
        if path.is_dir():
            yield from find_deployables(
                config=config,
                paths=(path for path in path.iterdir() if path.name[0] not in "._"),
                guess_destination=False,
            )
            continue
        if not path.is_file():
            continue
        lines = path.read_text("utf8", "ignore").splitlines()
        if path.suffix != ".py" and not any(
            lines[0].startswith(shebang) for shebang in PYTHON_SHEBANGS
        ):
            continue
        destinations: list[str] = []
        for line in lines[:10]:
            if line.startswith("DEPLOY_TARGET = ") or line.startswith("DEPLOYMENT_DESTINATION = "):
                destinations.append(literal_eval(line.split(" = ", 1)[1]))
            elif line.startswith(DESTINATION_MARKER):
                destinations.append(line.removeprefix(DESTINATION_MARKER))
        for destination in destinations:
            yield (path, Path(destination))
        if not destinations and guess_destination:
            yield (path, path.relative_to(path.parent))


# TODO: split that thing to Context and Config classes
# maybe I should use contextvars for root and src...
@dataclass
class Config:
    root: Path
    src: Path

    file_header: str
    templates: dict[str, Path]
    preship: list[list[str]]
    sources: list[Path]
    targets: list[Path]
    archive: list[Path]
    backups: list[Path]

    _backup_dirs: list[Path] | None = None

    @property
    def backup_dirs(self) -> list[Path]:
        now = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        if self._backup_dirs is None:
            backup_dirs = []
            for path in self.backups:
                if not path.is_absolute():
                    path = self.root / path
                path = path / f"{now}"
                path.mkdir(parents=True, exist_ok=True)
                backup_dirs.append(path)
            self._backup_dirs = backup_dirs
        return self._backup_dirs

    @staticmethod
    def from_dict(root: Path, config: dict[str, Any]) -> Config:
        src_dir = root / "src" if (root / "src").is_file() else root
        templs = {name: Path(str(path)) for name, path in subdict(config, "templates").items()}
        preship = [parse_command(cmd) for cmd in config.get("prerequisites", [])]
        sources = [src_dir.relative_to(root)]
        sources = [Path(str(path)) for path in config.get("deployables", sources)]
        file_header = config.get("file_header", "")
        targets = [Path(str(path)) for path in config.get("destinations", [".."])]
        archive = [Path(str(path)) for path in config.get("archives", [])]
        backups = [Path(str(path)) for path in config.get("backups", [])]
        for key in config:
            # TODO: implicit fields
            if key not in {
                "templates",
                "prerequisites",
                "deployables",
                "file_header",
                "destinations",
                "archives",
                "backups",
            }:
                eprint(f"Encountered unsupported key {key!r} in pyproject.toml")
        # TODO: implicit fields
        return Config(
            root=root,
            src=src_dir,
            file_header=file_header,
            templates=templs,
            preship=preship,
            sources=sources,
            targets=targets,
            archive=archive,
            backups=backups,
        )

    def to_toml(self) -> str:
        # TODO: implicit fields
        config = [
            "templates = " + tomlify(self.templates),
            "prerequisites = " + tomlify(self.preship),
            "deployables = " + tomlify(self.sources),
            "file_header = " + tomlify(self.file_header),
            "destinations = " + tomlify(self.targets),
            "archives = " + tomlify(self.archive),
            "backups = " + tomlify(self.backups),
        ]
        return "\n".join(config)


# TODO @dataclass (SourceFile): destinaton...
# and in TOML: sources = ["path1", {source="path2", destination="path3"}]


def run_tests(root: Path, tests: list[str] | list[list[str]]) -> list[str]:
    errors = []
    for test in [tests] if isinstance(tests, str) else tests:
        cmd = test.split() if isinstance(test, str) else test.copy()
        if cmd[0].endswith(".py"):
            cmd = ["python"] + cmd
        tprint("> " + " ".join(cmd))
        if cmd[0] == "python":
            cmd[0] = sys.executable
        if run(
            cmd,
            cwd=root,
            stdin=sys.stdin,
            stdout=sys.stdout,
            stderr=sys.stderr,
            check=False,
        ).returncode:
            errors.append("> " + " ".join(cmd))
    return errors


def get_dependencies(path: Path) -> list[Path]:
    queue = [path]
    deps: dict[Path, str] = {}
    if path.suffixes == [".py"]:
        stubfile = path.with_suffix(".pyi")
        if stubfile.exists():
            deps[stubfile] = path.stem
    while queue:
        path = queue.pop(0)
        for line in path.read_text("utf-8", "strict").splitlines():
            line = line.strip()
            if line.startswith("import ") or line.startswith("from "):
                mod_name = line.split()[1]
                mod_path = path.parent / f"{mod_name}.py"
                if mod_path.is_file() and mod_path not in deps:
                    deps[mod_path] = mod_name
                    queue.append(mod_path)
            elif " = Path(__file__).parent / " in line:
                path = Path(path.parent, literal_eval(line.split("/", 1)[1]))
                deps[path] = line.split()[0]
    return list(deps.keys())


def find_file(root: Path, name: str) -> Path | None:
    root = root.resolve()
    for path in chain([root], root.parents):
        path = Path(path, name)
        if path.exists():
            return path
    return None


def read_file(config: Config, path: Path) -> bytes:
    data = path.read_bytes()
    if not config.file_header:
        return data
    lines = data.decode(errors="replace").splitlines()
    encoded = "".join(line + "\n" for line in lines).encode()
    if encoded != data:
        return data
    if lines[0].startswith(DESTINATION_MARKER):
        lines[0] = config.file_header
    encoded = "".join(line + "\n" for line in lines).encode()
    return encoded


def tomlify(obj: object) -> str:
    lst: list[object]
    mapping: dict[str, object]
    match obj:
        case list() as lst:
            return "[" + ", ".join(map(tomlify, lst)) + "]"
        case dict() as mapping:
            items: list[str] = []
            for key, value in mapping.items():
                assert isinstance(key, str) and all(ch in IDENTIFIER_CHARS for ch in key)
                items.append(f"{key} = {tomlify(value)}")
            return "{" + ", ".join(items) + "}"
        case Path() as path:
            string = str(path)
            return f"'{string}'" if "'" not in string else tomlify(string)
        case str() as string:
            return dumps(string, ensure_ascii=False)
        case float() as number:
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
