"""Render per-component READMEs into the docs site as a generated page tree.

Ownership (see docs/mkdocs.yml): a component's own README.md, inside the
component's own repository, is the source of truth for that component. This
hook turns that tree into site pages so the docs render it — no README content
is duplicated or hand-copied into docs/.

How it works
------------
- Every directory under COMPONENT_ROOTS becomes a nav section. Its README.md
  becomes the section index; any other markdown file below it becomes a child
  page at its original relative path. Directories that have real files but no
  documentation get a stub page, so the tree always mirrors the repo.
- Pages are added to mkdocs' virtual file set (File.generated) — nothing is
  written to the working tree, so there is no generated/ directory to gitignore
  and no stale copy to clean.
- Nav is generated from the same walk, so adding a service or a README needs
  no docs edits. Cross-cutting pages stay hand-written in mkdocs.yml.
- Relative links inside READMEs are rewritten for their new location: a link
  to another documented path becomes a link to that page; a link to a
  non-markdown file (compose.yaml, topics.yaml, an image) becomes an absolute
  link to the repository that actually owns that file — which, for files inside
  a submodule, is the submodule's own repo, not the platform repo.
"""

from __future__ import annotations

import configparser
import pathlib
import posixpath
import re
import subprocess
from dataclasses import dataclass, field
from typing import Any

from mkdocs.structure.files import File
from mkdocs.structure.nav import Section

# Repo roots whose subtree is documented here. `shared/` is intentionally
# absent until it holds something real.
COMPONENT_ROOTS = ("services", "jobs", "platform", "infrastructure")

SKIP_DIRS = {
    ".git", ".github", ".venv", "venv", "node_modules", "__pycache__",
    ".mypy_cache", ".pytest_cache", ".ruff_cache", "target", "dist", "build",
    "site", "tmp", "assets", "models", "macros", "seeds", "snapshots",
    "dbt_packages", "logs", "logs_dbt",
}

IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".avif"}
LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^)\s]+)((?:\s+\"[^\"]*\")?\))")
H1_RE = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)

PLATFORM_REPO = "Cortajarena/hyperdata-platform"

# Populated per build by _scan(); module-level so the nav hook and the file
# hook (which mkdocs calls separately) share one walk.
_state: dict[str, Any] = {}


# --------------------------------------------------------------------------
# repo plumbing
# --------------------------------------------------------------------------
def _submodules(root: pathlib.Path) -> dict[str, str]:
    """Map submodule path -> 'owner/repo', from .gitmodules."""
    mods: dict[str, str] = {}
    gm = root / ".gitmodules"
    if not gm.is_file():
        return mods
    parser = configparser.ConfigParser()
    try:
        parser.read(gm)
    except configparser.Error:
        return mods
    for section in parser.sections():
        path = parser.get(section, "path", fallback=None)
        url = parser.get(section, "url", fallback=None)
        if not path or not url:
            continue
        m = re.search(r"[:/]([^/:]+/[^/]+?)(?:\.git)?$", url)
        if m:
            mods[path.strip("/")] = m.group(1)
    return mods


def _owner_repo(path: str, mods: dict[str, str]) -> str:
    """Which GitHub repo owns this path: a submodule's, or the platform's."""
    best = ""
    for sub in mods:
        if (path == sub or path.startswith(sub + "/")) and len(sub) > len(best):
            best = sub
    return mods[best] if best else PLATFORM_REPO


def _normalize(base: pathlib.PurePosixPath, target: str) -> str | None:
    """Resolve a relative markdown target against `base`; None if it escapes the repo."""
    parts: list[str] = []
    for part in (base / target).parts:
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                return None
            parts.pop()
        else:
            parts.append(part)
    return "/".join(parts)


PROPER_NOUNS = {"dbt": "dbt", "gcp": "GCP", "kafka": "Kafka", "kind": "KinD"}


def _humanize(name: str) -> str:
    if name in PROPER_NOUNS:
        return PROPER_NOUNS[name]
    return name.replace("-", " ").replace("_", " ").title()


def _title(text: str, fallback: str) -> str:
    """Nav label for a page: its H1, minus any trailing parenthetical."""
    m = H1_RE.search(text)
    if not m:
        return _humanize(fallback)
    title = re.sub(r"\s*\([^)]*\)\s*$", "", m.group(1).strip())
    return title or _humanize(fallback)


# --------------------------------------------------------------------------
# walk
# --------------------------------------------------------------------------
@dataclass
class Page:
    """A node in the components tree.

    url=None -> a grouping section with no page of its own (a directory that has
    no README but does hold documentation further down, e.g. a component with a
    docs/ or scripts/ folder). src=None -> a page we synthesize (the
    'README pending' stub for an undocumented component).
    """
    title: str
    url: str | None = None         # posix, relative to the site root, ends with .md
    src: pathlib.Path | None = None
    kind: str = "readme"           # readme | stub | intro
    children: list = field(default_factory=list)   # list[Page]


def _has_real_files(directory: pathlib.Path) -> bool:
    """A dir counts as real if it holds a non-dot file somewhere below."""
    for path in directory.rglob("*"):
        if path.is_file() and not path.name.startswith("."):
            if not any(part in SKIP_DIRS for part in path.relative_to(directory).parts[:-1]):
                return True
    return False


def _walk(directory: pathlib.Path, root: pathlib.Path, pages_by_src: dict[str, str]) -> Page | None:
    """Build the tree for one directory: its README (or a stub) plus child nodes.

    A directory is a *component* when it is a COMPONENT_ROOT or sits directly
    under one — those get a "README pending" stub when undocumented. Deeper
    directories (src/, config/, scripts/) are only nodes when they actually
    hold documentation, so no stub pages appear for plain source folders.
    """
    rel_dir = directory.relative_to(root).as_posix() if directory != root else ""
    readme = directory / "README.md"
    is_component = rel_dir in COMPONENT_ROOTS or rel_dir.count("/") == 1

    child_dirs: list[Page] = []
    child_pages: list[Page] = []
    for child in sorted(directory.iterdir(), key=lambda p: p.name):
        if child.name.startswith(".") or child.name in SKIP_DIRS:
            continue
        if child.is_dir():
            sub = _walk(child, root, pages_by_src)
            if sub is not None:
                child_dirs.append(sub)
        elif child.suffix == ".md" and child.name != "README.md":
            rel = child.relative_to(root).as_posix()
            url = f"components/{rel}"
            pages_by_src[rel] = url
            child_pages.append(
                Page(
                    title=_title(child.read_text(encoding="utf-8", errors="replace"), child.stem),
                    url=url,
                    src=child,
                )
            )

    if readme.is_file():
        node = Page(
            title=_title(readme.read_text(encoding="utf-8", errors="replace"), directory.name),
            url=f"components/{rel_dir}/index.md" if rel_dir else "components/index.md",
            src=readme,
        )
        pages_by_src[readme.relative_to(root).as_posix()] = node.url
        pages_by_src[rel_dir] = node.url
    elif child_dirs or child_pages:
        # Documented below, but nothing to render here: a nav section only.
        node = Page(title=_humanize(directory.name))
    elif is_component and _has_real_files(directory):
        # A real component with no docs yet: show it, marked as pending.
        node = Page(title=_humanize(directory.name),
                    url=f"components/{rel_dir}/index.md", src=None, kind="stub")
        pages_by_src[rel_dir] = node.url
    else:
        return None

    node.children = sorted(child_dirs + child_pages, key=lambda n: n.title.lower())
    return node


def _scan(root: pathlib.Path, docs_dir: pathlib.Path) -> tuple[list[Page], dict[str, str]]:
    """Component-root sections + a map of source path -> page url for link rewriting."""
    pages_by_src: dict[str, str] = {}
    sections: list[Page] = []

    # Hand-written pages count as link targets too, so a README can point at them.
    # Registered under both the docs_dir-relative and the repo-root-relative name:
    # a README under services/x/ links ../../docs/ingestion.md, not ../../ingestion.md.
    for md in sorted(docs_dir.glob("*.md")):
        pages_by_src[md.name] = md.name
        pages_by_src[f"docs/{md.name}"] = md.name

    for name in COMPONENT_ROOTS:
        directory = root / name
        if not directory.is_dir():
            continue
        section = _walk(directory, root, pages_by_src)
        if section is not None:
            section.title = name
            sections.append(section)
    # /components/ itself needs an index, so the nav section has a landing page.
    sections.insert(0, Page(title="Overview", url="components/index.md", kind="intro"))
    return sections, pages_by_src


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------
def _render(page: Page, root: pathlib.Path, mods: dict[str, str], pages_by_src: dict[str, str]) -> str:
    assert page.url is not None                      # only page-bearing nodes render
    if page.kind == "intro":
        return _render_intro()
    if page.src is None:
        rel_dir = pathlib.PurePosixPath(page.url).parent.as_posix()
        rel_dir = rel_dir[len("components/"):] if rel_dir.startswith("components/") else rel_dir
        repo = _owner_repo(rel_dir, mods)
        link = f"https://github.com/{repo}/tree/main/{rel_dir}"
        return (
            f"# {page.title}\n\n"
            '!!! note "README pending"\n'
            f"    This component has no `README.md` yet — the source of truth for it does not exist.\n"
            f"    Component path: [`{rel_dir}`]({link})\n"
        )

    text = page.src.read_text(encoding="utf-8", errors="replace")
    text = _rewrite_links(text, page.src.relative_to(root).parent, page.url, mods, pages_by_src)
    return text


_INTRO = """# Components

Every page under **Components** is rendered from a `README.md` that sits next to the code it
documents — usually in that component's own repository, versioned with it. Nothing here is
hand-copied: add a README (or a whole service) and this tree updates itself on the next build.

| Section | What lives there |
| :--- | :--- |
| `services/` | Long-running deployables — the node, its sidecar, the indexers |
| `jobs/` | Compute workloads by engine — Flink, dbt, Spark |
| `platform/` | Cluster systems — Kafka, the Flink cluster, orchestration, serving |
| `infrastructure/` | Where things run — local KinD, GCP terraform |

A component with no `README.md` yet still appears, marked *README pending* — the tree mirrors
the repository rather than the documentation.

Design that cuts across components — how they fit together, and why — lives in
[Ingestion](../ingestion.md) and [Transformation](../transformation.md). The repo-wide
orientation is the [root README](https://github.com/Cortajarena/hyperdata-platform).
"""


def _render_intro() -> str:
    return _INTRO


def _rewrite_links(text: str, base: pathlib.PurePosixPath, current_url: str,
                   mods: dict[str, str], pages_by_src: dict[str, str]) -> str:
    def replace(match: re.Match) -> str:
        head, target, tail = match.group(1), match.group(2), match.group(3)
        if target.startswith(("http://", "https://", "#", "mailto:", "tel:", "data:")):
            return match.group(0)
        path_part, sep, fragment = target.partition("#")
        if not path_part:
            return match.group(0)

        resolved = _normalize(base, path_part)
        if resolved is None:                       # points outside the repo: leave it
            return match.group(0)

        anchor = f"#{fragment}" if sep else ""
        page_url = pages_by_src.get(resolved) or pages_by_src.get(resolved.rstrip("/"))
        if page_url:
            here = posixpath.dirname(current_url) or "."
            rel = posixpath.relpath(page_url, here)
            if not rel.startswith("."):
                rel = f"./{rel}"
            return f"{head}{rel}{anchor}{tail}"

        if (_state["root"] / resolved).exists():
            repo = _owner_repo(resolved, mods)
            if pathlib.PurePosixPath(resolved).suffix.lower() in IMAGE_SUFFIXES:
                url = f"https://raw.githubusercontent.com/{repo}/main/{resolved}"
            else:
                url = f"https://github.com/{repo}/blob/main/{resolved}"
            return f"{head}{url}{anchor}{tail}"
        return match.group(0)                       # dangling: leave it visible

    return LINK_RE.sub(replace, text)


def _nav_items(sections: list[Page]) -> list:
    def to_item(page: Page):
        if not page.children:
            return (page.url, page.title)
        return Section(page.title, [to_item(c) for c in page.children])

    return [to_item(s) for s in sections]


# --------------------------------------------------------------------------
# mkdocs hooks
# --------------------------------------------------------------------------
def on_config(config, **kwargs):
    docs_dir = pathlib.Path(config["docs_dir"])
    root = docs_dir.parent
    sections, pages_by_src = _scan(root, docs_dir)
    _state["root"] = root
    _state["docs_dir"] = docs_dir
    _state["mods"] = _submodules(root)
    _state["pages"] = sections
    _state["pages_by_src"] = pages_by_src
    return config


def on_files(files, config, **kwargs):
    root: pathlib.Path = _state["root"]
    mods: dict[str, str] = _state["mods"]
    pages_by_src: dict[str, str] = _state["pages_by_src"]

    def walk(page: Page) -> None:
        if page.url is not None:
            content = _render(page, root, mods, pages_by_src)
            files.append(File.generated(config, page.url, content=content))
        for child in page.children:
            walk(child)

    for section in _state["pages"]:
        walk(section)
    return files


def on_nav(nav, config, files, **kwargs):
    """Append the generated Components tree to the hand-written nav.

    Runs after on_files, so the generated pages exist and can be resolved to
    real Page objects (mkdocs 1.6 nav items are Page/Section instances, not
    (path, title) tuples).
    """
    def to_item(page: Page):
        children = [item for item in (to_item(c) for c in page.children) if item is not None]
        if page.url is None:                                  # grouping section, no page
            return Section(page.title, children) if children else None
        built = files.get_file_from_path(page.url)
        if not isinstance(built, File):                        # pragma: no cover
            return None
        built.title = page.title
        return Section(page.title, children) if children else built

    items = [item for item in (to_item(s) for s in _state["pages"]) if item is not None]
    if items:
        nav.items.append(Section("Components", items))
    return nav
